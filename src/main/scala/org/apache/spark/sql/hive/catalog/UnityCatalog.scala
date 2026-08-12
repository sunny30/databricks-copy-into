package org.apache.spark.sql.hive.catalog

import org.apache.hadoop.fs.Path
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.spark.{Spark3Util, SparkFunctionCatalog, SparkSchemaUtil}
import org.apache.iceberg.spark.functions.SparkFunctions
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStatistics, CatalogTable, CatalogTableType, CatalogUtils, ExternalCatalog}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.connector.catalog
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, CatalogV2Util, Identifier, NamespaceChange, StagedTable, StagingTableCatalog, SupportsNamespaces, SupportsWrite, Table, TableCapability, TableCatalog, TableChange, TableSchemaChangeCatalog, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.iceberg.catalog.{Procedure, ProcedureCatalog}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.delta.{DeltaErrors, UnityDeltaCatalog}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.hive.catalog.cls.ExternalSecureCatalog
import org.apache.spark.sql.hive.plan.CLSUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.hudi.UnityHudiCatalog
import org.apache.spark.sql.iceberg.UnityIcebergCatalog

import scala.collection.JavaConverters._
import java.net.URI
import java.util
import scala.collection.JavaConverters.{asJavaIterableConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`map AsScala`

class UnityCatalog[T <: TableCatalog with SupportsNamespaces] extends CatalogExtension
  with SupportsNamespaces with ProcedureCatalog
  with StagingTableCatalog with DeltaLogging with SQLConfHelper with TableSchemaChangeCatalog with HasIcebergCatalog {

  private var catalogName: String = null

  private var delegatedCatalog: CatalogPlugin = null

  var options: CaseInsensitiveStringMap = null

  lazy val externalCatalog: ExternalSecureCatalog = if (SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")) {
    new FSMetaStoreCatalog(
      catalogName,
      sparkConf = SparkSession.active.sharedState.conf,
      hadoopConfig = SparkSession.active.sharedState.hadoopConf
    )
  } else {
    new HMSCatalog(
      catalogName,
      sparkConf = SparkSession.active.sharedState.conf,
      hadoopConfig = SparkSession.active.sharedState.hadoopConf
    )
  }

  var proxyCatalog: ProxyCatalog = null;

  private var liveCatalog: LiveCatalog[_ <: TableCatalog with SupportsNamespaces] = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // TODO
    log.info("Inside Catalog Plugin Initialize")
    // Initialize the catalog with the corresponding name
    if (name.equalsIgnoreCase("hive")) {
      this.catalogName = "cat"
    } else {
      this.catalogName = name
    }
    this.options = options
    proxyCatalog = new ProxyCatalog(catalogName = catalogName, proxyDBName = None)
    // Initialize the catalog in any other provider that we can integrate with
    liveCatalog = new LiveCatalog()
    liveCatalog.initialize(name, options)
  }

  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = {
    // TODO: lOGS
    log.info("Inside set Delegated of Catalog Extension")
    // Check if the delegating catalog has Table and SupportsNamespace properties
    if (delegate.isInstanceOf[TableCatalog] && delegate.isInstanceOf[SupportsNamespaces]) {
      this.delegatedCatalog = delegate
      // Set delegated catalog in any other provider that we can integrate with
    } else throw new IllegalArgumentException("Invalid session catalog: " + delegate)
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = {
    if (externalCatalog.tableExists(db, table) && stats.isDefined) {
      externalCatalog.alterTableStats(db, table, stats)
    }
  }

  override def getTableStats(db: String, table: String): Option[CatalogStatistics] ={
    externalCatalog.getTable(db, table).stats
  }


  override def alterUnsafeCatalogTable(ct: CatalogTable): Unit = {
    externalCatalog.alterUnsafeCatalogTable(ct)
  }


  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if(isLiveCatalog){
      return liveCatalog.listTables(namespace)
    }
    namespace match {
      case Array(db) =>
        if (proxyCatalog.databaseExists(db)) {
          proxyCatalog.listTables(db).map(tb => TableIdentifier(tb, Some(db)))
            .map(ident => Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
            .toArray
        } else {
          externalCatalog.listTables(db).map(tb => TableIdentifier(tb, Some(db)))
            .map(ident => Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
            .toArray
        }
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def getTableLocation(db: String, table: String): String = {
    if (externalCatalog.tableExists(db, table))
      externalCatalog.getTable(db, table).storage.locationUri.get.toString
    else
      null
  }

  override def alterTable(ident: Identifier, schema: StructType): Unit = {

    val catalogTable = try {
      externalCatalog.getTable(ident.asTableIdentifier.database.getOrElse("default"), ident.asTableIdentifier.table)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

    externalCatalog.alterTable(
      catalogTable.copy(
        schema = schema))


  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val catalogTable = try {
      externalCatalog.getTable(ident.asTableIdentifier.database.getOrElse("default"), ident.asTableIdentifier.table)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

    if (catalogTable.provider.isDefined) {
      if (catalogTable.provider.get.equalsIgnoreCase("delta")) {
        val dt =  (new UnityDeltaCatalog(externalCatalog,catalogName)).alterTable(ident, changes)
        alterTableV2Table(catalogTable, ident, changes)
        dt
      }
      else if (catalogTable.provider.get.equalsIgnoreCase("iceberg")) {
        val tbl  = (new UnityIcebergCatalog(externalCatalog, catalogName, options)).alterTable(ident, changes: _*)
        alterTableV2Table(catalogTable, ident, changes)
        tbl
      } else {
        alterTableV2Table(catalogTable, ident, changes)
      }
    }else{
      alterTableV2Table(catalogTable, ident, changes)
    }



  }


  private def alterTableV2Table(catalogTable: CatalogTable, ident: Identifier, changes: Seq[TableChange]):Table ={
    val properties = CatalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(
      catalogTable.schema, changes, catalogTable.provider, "ALTER TABLE")
    val comment = properties.get(TableCatalog.PROP_COMMENT)
    val owner = properties.getOrElse(TableCatalog.PROP_OWNER, catalogTable.owner)
    val location = properties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
    val storage = if (location.isDefined) {
      catalogTable.storage.copy(locationUri = location)
    } else {
      catalogTable.storage
    }
    val table = if (loadTable(ident) == null) {
      loadTable(ident, null)
    } else {
      loadTable(ident)
    }
    table match {
      case deltaTableV2: DeltaTableV2 => (new UnityDeltaCatalog(externalCatalog, catalogName)).alterTable(ident, changes)
      case _ => try {
        val newProvider = properties.getOrElse("spark.sql.sources.provider", catalogTable.provider.getOrElse("csv"))
        externalCatalog.alterTable(
          catalogTable.copy(
            provider = Some(newProvider),
            properties = properties, schema = schema, owner = owner, comment = comment,
            storage = storage))

        V2Table(catalogTable)
      } catch {
        case _: NoSuchTableException =>
          throw QueryCompilationErrors.noSuchTableError(ident)
      }
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    externalCatalog.dropTable(dbName, tableName, true, false)
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldTableName = oldIdent.asTableIdentifier.table
    val newTableName = newIdent.asTableIdentifier.table
    val dbName = newIdent.asTableIdentifier.database.getOrElse("default")
    val providerValue = externalCatalog.getTable(dbName,oldTableName).provider
    providerValue match {
      case Some(value) if(value.equalsIgnoreCase("iceberg"))=> (new UnityIcebergCatalog(externalCatalog, catalogName, options)).renameTable(oldIdent, newIdent)
      case _ => externalCatalog.renameTable(db = dbName, oldName = oldTableName, newName = newTableName)
    }

  }

  override def listNamespaces(): Array[Array[String]] = {
    if (isLiveCatalog)
      return liveCatalog.listNamespaces()
    (externalCatalog.
      listDatabases() ++ proxyCatalog.listDatabase()).
      map(x => Array(x)).
      toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (isLiveCatalog)
      return liveCatalog.listNamespaces(namespace)
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if (externalCatalog.databaseExists(db) || proxyCatalog.databaseExists(db)) =>
        Array()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if(isLiveCatalog)
      return liveCatalog.loadNamespaceMetadata(namespace)

    namespace match {
      case Array(db) =>
        val properties = if (proxyCatalog.databaseExists(db)) {
          proxyCatalog.getDatabase(db).properties.asJava
        } else {

          val augmentedProperties = externalCatalog.getDatabase(db).properties ++ Map("db_location" -> externalCatalog.getDatabase(db).locationUri.toString)
          augmentedProperties.asJava
        }
        properties

//      case Array(db,table) =>
//        val augmentedProperties = externalCatalog.getDatabase(db).properties ++ Map("db_location" -> externalCatalog.getDatabase(db).locationUri.toString)
//        augmentedProperties.asJava

      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
                                namespace: Array[String],
                                metadata: util.Map[String, String]): Unit = {
    val cd = namespace match {
      case Array(db) => toCatalogDatabase(db, metadata)
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
    externalCatalog.createDatabase(cd, false)

  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    namespace match {
      case Array(db) =>
        try {
          externalCatalog.dropDatabase(db, false, cascade)
          true
        } catch {
          case e: Exception => false
        }
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    if (isLiveCatalog)
      return liveCatalog.namespaceExists(namespace)
    namespace match {
      case Array(db) => externalCatalog.databaseExists(db)
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def defaultNamespace(): Array[String] = super.defaultNamespace()



  override def registerTableInMetastore(table: CatalogTable,ignoreIfExists:Boolean=false): Unit = {
    val dbPath = getDBPath(table.database)
    val dbStringPath = if (dbPath.toString.endsWith("/")) {
      dbPath.toString
    } else {
      dbPath.toString + "/"
    }
    var location = table.storage.locationUri
    location = location match {
      case None =>
        Some(CatalogUtils.stringToURI(dbStringPath + table.identifier.table))
      case Some(v) => Some(v)

    }
    val newtable = table.copy(storage = table.storage.copy(locationUri = location))
    externalCatalog.createTable(newtable, ignoreIfExists)
  }






  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {

    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper

    var isExternal = false
    var location = Option(properties.get(TableCatalog.PROP_LOCATION))
    var dbPath = getDBPath(ident.namespace.apply(0))
    val dbStringPath = if (dbPath.toString.endsWith("/")) {
      dbPath.toString
    } else {
      dbPath.toString + "/"
    }
    location = location match {
      case None =>
        Some(dbStringPath + ident.name)
      case _ =>
        isExternal = true
        location
    }
    isExternal = isExternal || properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val locationUri = location.map(CatalogUtils.stringToURI)

    val provider = getProvider(properties)

    if (provider.equalsIgnoreCase("delta")) {
      new UnityDeltaCatalog(externalCatalog,catalogName).createDeltaTable(
        ident,
        schema,
        partitions,
        properties,
        Map.empty,
        sourceQuery = None,
        TableCreationModes.Create,
        location.getOrElse(""),
        isExternal
      )

    } else {
      val (partitionColumns, maybeBucketSpec) =
        if (provider.equalsIgnoreCase("iceberg")) {
          val icebergSchema = SparkSchemaUtil.convert(schema)
          val ps = Spark3Util.toPartitionSpec(icebergSchema, partitions)
          (ps.fields().asScala.map(p => p.name()), None)
        } else {
          partitions.toSeq.convertTransforms
        }

      val tableProperties = properties.asScala
      val inputLocation = Option(properties.get(TableCatalog.PROP_LOCATION))
      var isExternal = inputLocation.isDefined
      val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties.toMap))
        .copy(locationUri = location.map(CatalogUtils.stringToURI))
      isExternal = isExternal || properties.containsKey(TableCatalog.PROP_EXTERNAL)
      val tableType = if (isExternal) {
        CatalogTableType.EXTERNAL
      } else {
        CatalogTableType.MANAGED
      }

      val tableDesc = CatalogTable(
        identifier = ident.asTableIdentifier,
        tableType = tableType,
        storage = storage,
        schema = schema,
        provider = Some(provider),
        partitionColumnNames = partitionColumns,
        bucketSpec = maybeBucketSpec,
        properties = tableProperties.toMap,
        tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
        comment = Option(properties.get(TableCatalog.PROP_COMMENT)))
      try {

        if (provider.equalsIgnoreCase("iceberg")) {
          var icebergProperties = (properties.asScala.toMap ++ Map(TableCatalog.PROP_LOCATION -> location.get))
          if(isExternal){
            icebergProperties = icebergProperties ++  Map(TableCatalog.PROP_EXTERNAL -> "true")
          }
          val icebergCatalog = new UnityIcebergCatalog(externalCatalog, catalogName, options)
          return icebergCatalog.createIcebergTable(ident, schema, partitions, icebergProperties.asJava, tableDesc)
        }
        if(provider.equalsIgnoreCase("hudi")){
          val hudiCatalog = new UnityHudiCatalog(externalCatalog, catalogName)
          return hudiCatalog.createHudiTable(ident, schema, partitions, properties,tableDesc)
        }
        externalCatalog.createTable(tableDesc, ignoreIfExists = true)
        if (tableType == CatalogTableType.VIEW) {
          V2Table(tableDesc)
        } else {
          loadTable(ident)
        }
      } catch {
        case e: Exception => throw e
      }
    }
  }

  private def getProvider(properties: util.Map[String, String]):String = {
    val hiveStoredAsKey = "hive.stored-as"
    val provider = properties.asScala.get(TableCatalog.PROP_PROVIDER) match {
      case Some(value) => value
      case None =>
        if(properties.containsKey(hiveStoredAsKey)){
          properties.asScala.get(hiveStoredAsKey).get
        }else{
          conf.defaultDataSourceName
        }
    }
    provider
  }

  def createTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit = {
    externalCatalog.createTable(tableDesc, ignoreIfExists)
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }

  override def tableExists(ident: Identifier): Boolean = {
    if(isLiveCatalog)
      return  liveCatalog.tableExists(ident)
    try {
      loadTable(ident) != null || loadTable(ident, null) != null
    } catch {
      case e: NoSuchTableException =>
        false
    }
  }


  override def createTable(ident: Identifier, columns: Array[catalog.Column], partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    createTable(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)
  }

  override def loadTable(ident: Identifier): Table = {
    if(isLiveCatalog){
      return liveCatalog.loadTable(ident)
    }
    if (ident.namespace().size > 1) {
      //this if block for history,snapshots..so far its only possible for
      new UnityIcebergCatalog(externalCatalog, catalogName, options).loadTable(ident)
    } else {
      val tableName = ident.asTableIdentifier.table
      val dbName = ident.asTableIdentifier.database.getOrElse("default")
      val tt = if (proxyCatalog.tableExists(db = dbName, table = tableName)) {
        proxyCatalog.getTable(db = dbName, table = tableName)
      } else {
        externalCatalog.getTable(table = tableName, db = dbName)
      }

      if (tt == null)
        return null
      var resultTable: Table = null
      resultTable = if (tt.provider.isDefined && tt.provider.get.equalsIgnoreCase("delta")) {
        val emptySchemaTable = tt.copy(
          schema = new StructType(),
          partitionColumnNames = Seq.empty,
          bucketSpec = None // BucketSpec also references column names
        )
        DeltaTableV2(
          SparkSession.active,
          new Path(tt.location),
          catalogTable = Some(emptySchemaTable),
          tableIdentifier = Some(ident.toString))
      } else if (tt.provider.isDefined && tt.provider.get.equalsIgnoreCase("iceberg")) {
        new UnityIcebergCatalog(externalCatalog, catalogName, options).loadTable(ident)
      } else if(tt.provider.isDefined && tt.provider.get.equalsIgnoreCase("hudi")){
        new UnityHudiCatalog(externalCatalog, catalogName).loadTable(ident)
      } else{
        if (tt != null && tt.tableType == CatalogTableType.VIEW) {
          return null
        }
        if (tt != null) {
          V2Table(tt)
        } else {
          null
        }
      }
      CLSUtils.syncSchemaAtLoadAndOverWrite(resultTable, loadSecureTable(dbName,tableName),catalogName)
      resultTable
    }
  }

  override def loadSecureTable(db: String, table: String): CatalogTable = {
    if(isLiveCatalog){
      liveCatalog.loadTable(Identifier.of(Array(db), table))
    }
    externalCatalog.getSecureTable(db, table)
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    val tt = externalCatalog.getTable(table = tableName, db = dbName)
    if (timestamp == null) {
      if (tt != null) {
        V2Table(tt)
      } else {
        null
      }
    } else {
      val trueTable = loadTable(ident)
      val secureTable = loadSecureTable(dbName, tableName)
      if(!CLSUtils.sameFieldsUnordered(trueTable.schema(), secureTable.schema)){
        throw new IllegalArgumentException("User with partial permission, not allowed for time travel")
      }
      tt.provider match {
        case Some(value) => if(value.equalsIgnoreCase("delta")){
          new UnityDeltaCatalog(externalCatalog,catalogName).loadTable(ident, timestamp)
        }else if(value.equalsIgnoreCase("iceberg")){
          new UnityIcebergCatalog(externalCatalog, catalogName, options).loadTable(ident, timestamp)
        }else{
          throw new IllegalArgumentException(s"${value} dataforat not supported")
        }
      }
    }
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    val tt = externalCatalog.getTable(table = tableName, db = dbName)
    if (version == null) {

      if (tt != null) {
        V2Table(tt)
      } else {
        null
      }
    } else {
      tt.provider match {
        case Some(value) => if (value.equalsIgnoreCase("delta")) {
          new UnityDeltaCatalog(externalCatalog,catalogName).loadTable(ident, version)
        } else if (value.equalsIgnoreCase("iceberg")) {
          new UnityIcebergCatalog(externalCatalog, catalogName, options).loadTable(ident, version)
        } else {
          throw new IllegalArgumentException(s"${value} dataforat not supported")
        }
      }
    }
  }


  override def loadFunction(ident: Identifier): UnboundFunction = (new SparkFunctionCatalog()).loadFunction(ident)


  override def listFunctions(namespace: Array[String]): Array[Identifier] =  (new SparkFunctionCatalog()).listFunctions(namespace)

  override def stageReplace(
                             ident: Identifier,
                             schema: StructType,
                             partitions: Array[Transform],
                             properties: util.Map[String, String]): StagedTable = {
    if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
      new UnityDeltaCatalog(externalCatalog,catalogName).stageReplace(ident, schema, partitions, properties)
    } else if (properties.containsKey("provider") && properties.get("provider").equalsIgnoreCase("iceberg")) {
      (new UnityIcebergCatalog(externalCatalog, catalogName, options)).stageReplace(ident, schema, partitions, properties)
    } else {
      dropTable(ident)
      val table = createTable(ident, schema, partitions, properties)
      BestEffortStagedTable(ident, table, this)
    }
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    println("Inside stageCreateOrReplace")
    if (DeltaSourceUtils.isDeltaDataSourceName(getProvider(properties))) {
      new UnityDeltaCatalog(externalCatalog,catalogName).stageCreateOrReplace(ident, schema, partitions, properties)
    } else if (properties.containsKey("provider") && properties.get("provider").equalsIgnoreCase("iceberg")) {
      (new UnityIcebergCatalog(externalCatalog, catalogName, options)).stageCrateOrReplace(ident, schema, partitions, properties)
    } else {
      dropTable(ident)
      val table = createTable(ident, schema, partitions, properties)
      BestEffortStagedTable(ident, table, this)
    }
  }

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {

   // val table = createTable(ident, schema, partitions, properties)
    if (properties.containsKey("provider") && properties.get("provider").equalsIgnoreCase("iceberg")) {
    //  val table = createTable(ident, schema, partitions, properties)
      (new UnityIcebergCatalog(externalCatalog, catalogName, options)).stageCrateOrReplace(ident, schema, partitions, properties)
    } else if (properties.containsKey("provider") && properties.get("provider").equalsIgnoreCase("delta")) {
      new UnityDeltaCatalog(externalCatalog,catalogName).stageCreate(ident, schema, partitions, properties)
    } else {
      val table = createTable(ident, schema, partitions, properties)
      BestEffortStagedTable(ident, table, this)
    }
  }

  override def name(): String = catalogName


  private def isLiveCatalog:Boolean={
    catalogName.equalsIgnoreCase("live")
  }

  private def toCatalogDatabase(
                                 db: String,
                                 metadata: util.Map[String, String],
                                 defaultLocation: Option[URI] = None): CatalogDatabase = {
    CatalogDatabase(
      name = db,
      description = metadata.getOrDefault(SupportsNamespaces.PROP_COMMENT, ""),
      locationUri = Option(metadata.get(SupportsNamespaces.PROP_LOCATION))
        .map(CatalogUtils.stringToURI)
        .orElse(defaultLocation)
        .getOrElse(getDBPath(db)),
      properties = metadata.asScala.toMap --
        Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
  }

  def getDBPath(db: String): URI = {
    val warehousePath = SparkSession.active.sharedState.conf.get("spark.sql.warehouse.dir")
    val catalogPath = new Path(warehousePath, catalogName + ".cat")
    val dbPath = new Path(catalogPath, db + ".db")
    dbPath.toUri
  }

  def loadProcedure(identifier: Identifier): Procedure = {
    new UnityIcebergCatalog(externalCatalog, catalogName, options).loadProcedure(identifier)
  }

  override def icebergCatalog(): Catalog = {
    new UnityIcebergCatalog(externalCatalog, catalogName, options)
  }
}


case class BestEffortStagedTable(
                                  ident: Identifier,
                                  table: Table,
                                  catalog: TableCatalog) extends StagedTable with SupportsWrite {
  override def abortStagedChanges(): Unit = catalog.dropTable(ident)

  override def commitStagedChanges(): Unit = {}

  // Pass through
  override def name(): String = table.name()

  override def schema(): StructType = table.schema()

  override def partitioning(): Array[Transform] = table.partitioning()

  override def capabilities(): util.Set[TableCapability] = table.capabilities()

  override def properties(): util.Map[String, String] = table.properties()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = table match {
    case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
    case _ => throw DeltaErrors.unsupportedWriteStagedTable(name)
  }



}

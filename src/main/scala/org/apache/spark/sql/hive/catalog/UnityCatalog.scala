package org.apache.spark.sql.hive.catalog

import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, CatalogTableType, CatalogUtils, ExternalCatalog}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.connector.catalog
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, CatalogV2Util, Identifier, NamespaceChange, StagedTable, StagingTableCatalog, SupportsNamespaces, Table, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._


import java.net.URI
import java.util
import scala.collection.JavaConverters.{asJavaIterableConverter, mapAsScalaMapConverter}
class UnityCatalog[T <: TableCatalog with SupportsNamespaces] extends CatalogExtension
  with SupportsNamespaces
  with StagingTableCatalog with DeltaLogging with SQLConfHelper{

  private var catalogName: String = null

  private var delegatedCatalog: CatalogPlugin = null

  private var externalCatalog: ExternalCatalog = if(SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")){
      new FSMetaStoreCatalog(
        catalogName,
        sparkConf = SparkSession.active.sharedState.conf,
        hadoopConfig = SparkSession.active.sharedState.hadoopConf
      )
  }else{
    SparkSession.active.sessionState.catalog.externalCatalog
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // TODO
    log.info("Inside Catalog Plugin Initialize")
    // Initialize the catalog with the corresponding name
    this.catalogName = name
    // Initialize the catalog in any other provider that we can integrate with
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


  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        externalCatalog.listTables(db).map(tb => TableIdentifier(tb, Some(db)))
          .map(ident => Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
          .toArray
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val catalogTable = try {
      externalCatalog.getTable(ident.asTableIdentifier.table, ident.asTableIdentifier.database.getOrElse("default"))
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }

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

    try {
      externalCatalog.alterTable(
        catalogTable.copy(
          properties = properties, schema = schema, owner = owner, comment = comment,
          storage = storage))

      V1Table(catalogTable)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    externalCatalog.dropTable(tableName, dbName, true,false)
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    val oldTableName = oldIdent.asTableIdentifier.table
    val newTableName = newIdent.asTableIdentifier.table
    val dbName = newIdent.asTableIdentifier.database.getOrElse("default")
    externalCatalog.renameTable(db = dbName, oldName = oldTableName, newName = newTableName)
  }
  override def listNamespaces(): Array[Array[String]] = {
      externalCatalog.
        listDatabases().
        map(x => Array(x)).
        toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if externalCatalog.databaseExists(db) =>
        Array()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) => externalCatalog.getDatabase(db).properties.asJava
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
                                namespace: Array[String],
                                metadata: util.Map[String, String]): Unit ={
    val cd = namespace match {
      case Array(db) => toCatalogDatabase(db, metadata)
      case  _ => throw  QueryCompilationErrors.noSuchNamespaceError(namespace)
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
        }catch {
          case e:Exception => false
        }
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    namespace match {
      case Array(db) => externalCatalog.databaseExists(db)
      case _ => throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def defaultNamespace(): Array[String] = super.defaultNamespace()

  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    (new DeltaCatalog).stageCreate(ident = ident, schema,partitions = partitions, properties = properties)
  }

//  override def stageReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
//    (new DeltaCatalog).stageCreate(ident = ident, columns,partitions = partitions, properties = properties)
//  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable = {
    (new DeltaCatalog).stageCreateOrReplace(ident = ident, schema = schema,partitions = partitions, properties = properties)
  }


  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {

    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
    val (partitionColumns, maybeBucketSpec) = partitions.toSeq.convertTransforms
    val provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, conf.defaultDataSourceName)
    val tableProperties = properties.asScala
    val location = Option(properties.get(TableCatalog.PROP_LOCATION))
    val storage = DataSource.buildStorageFormatFromOptions(toOptions(tableProperties.toMap))
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val tableType = if (isExternal || location.isDefined) {
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
      externalCatalog.createTable(tableDesc, ignoreIfExists = false)
      V1Table(tableDesc)
    }catch {
      case e: Exception => throw e
    }
  }

  private def toOptions(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value
    }.toMap
  }


  override def createTable(ident: Identifier, columns: Array[catalog.Column], partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    createTable(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)
  }

  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    V1Table(externalCatalog.getTable(table = tableName, db = dbName))
  }


  override def loadFunction(ident: Identifier): UnboundFunction = ???


  override def listFunctions(namespace: Array[String]): Array[Identifier] = ???

  override def stageReplace(
                             ident: Identifier,
                             schema: StructType,
                             partitions: Array[Transform],
                             properties: util.Map[String, String]): StagedTable = {
    (new DeltaCatalog).stageReplace(ident = ident, schema = schema, partitions = partitions, properties = properties)
  }
  override def name(): String = catalogName

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
        .getOrElse(throw QueryExecutionErrors.missingDatabaseLocationError()),
      properties = metadata.asScala.toMap --
        Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
  }

}

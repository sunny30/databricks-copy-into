package org.apache.spark.sql.hive.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType, CatalogUtils, ExternalCatalog, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_OWNER
import org.apache.spark.sql.hive.HiveUtils.{builtinHiveVersion, newTemporaryConfiguration}
import org.apache.spark.sql.hive.client.HiveClientImpl.{fromHiveColumn, getHive, newHiveConf, toHiveColumn, toHivePartition, toHiveTableType}
import org.apache.spark.sql.hive.client.{HiveClient, IsolatedClientLoader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{CircularBuffer, MutableURLClassLoader, Utils}
import org.apache.hadoop.hive.metastore.api.{Database => HiveDatabase, Table => MetaStoreApiTable, _}
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType => HiveTableType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchPartitionsException, NoSuchTableException}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException, Partition => HivePartition, Table => HiveTable}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hive.HiveExternalCatalog.{STATISTICS_COL_STATS_PREFIX, STATISTICS_NUM_ROWS, STATISTICS_TOTAL_SIZE}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._
import java.io.PrintStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HMSCatalog(
                  catalogName: String,
                  sparkConf: SparkConf,
                  hadoopConfig: Configuration = new Configuration,
                  sparkSession: SparkSession = SparkSession.active
                ) extends ExternalCatalog {

  lazy val msClient = getMSC(client)


  protected lazy val getMSCMethod = {
    // Since getMSC() in Hive 0.12 is private, findMethod() could not work here
    val msc = classOf[Hive].getDeclaredMethod("getMSC")
    msc.setAccessible(true)
    msc
  }

  lazy val clientLoader = new IsolatedClientLoader(
    version = IsolatedClientLoader.hiveVersion(builtinHiveVersion),
    sparkConf = sparkConf,
    execJars = Seq.empty,
    hadoopConf = sparkSession.sharedState.hadoopConf,
    config = newTemporaryConfiguration(useInMemoryDerby = true),
    isolationOn = false,
    baseClassLoader = Utils.getContextOrSparkClassLoader)
  private def client: Hive = {
    if (clientLoader.cachedHive != null) {
      clientLoader.cachedHive.asInstanceOf[Hive]
    } else {
      val c = getHive(conf)
      clientLoader.cachedHive = c
      c
    }
  }

  def conf: HiveConf = {
    val hiveConf = conf
    // Hive changed the default of datanucleus.schema.autoCreateAll from true to false
    // and hive.metastore.schema.verification from false to true since Hive 2.0.
    // For details, see the JIRA HIVE-6113, HIVE-12463 and HIVE-1841.
    // isEmbeddedMetaStore should not be true in the production environment.
    // We hard-code hive.metastore.schema.verification and datanucleus.schema.autoCreateAll to allow
    // bin/spark-shell, bin/spark-sql and sbin/start-thriftserver.sh to automatically create the
    // Derby Metastore when running Spark in the non-production environment.
    val isEmbeddedMetaStore = {
      val msUri = hiveConf.getVar(ConfVars.METASTOREURIS)
      val msConnUrl = hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY)
      (msUri == null || msUri.trim().isEmpty) &&
        (msConnUrl != null && msConnUrl.startsWith("jdbc:derby"))
    }
    if (isEmbeddedMetaStore) {
      hiveConf.setBoolean("hive.metastore.schema.verification", false)
      hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
    }
    hiveConf
  }

  private def newState(): SessionState = {
    val hiveConf = newHiveConf(sparkConf, sparkSession.sharedState.hadoopConf, Map.empty, None)
    val state = new SessionState(hiveConf)
    if (clientLoader.cachedHive != null) {
      Hive.set(clientLoader.cachedHive.asInstanceOf[Hive])
    }
    // Hive 2.3 will set UDFClassLoader to hiveConf when initializing SessionState
    // since HIVE-11878, and ADDJarsCommand will add jars to clientLoader.classLoader.
    // For this reason we cannot load the jars added by ADDJarsCommand because of class loader
    // got changed. We reset it to clientLoader.ClassLoader here.
    state.getConf.setClassLoader(clientLoader.classLoader)
    //shim.setCurrentSessionState(state)
    val outputBuffer = new CircularBuffer()
    state.out = new PrintStream(outputBuffer, true, UTF_8.name())
    state.err = new PrintStream(outputBuffer, true, UTF_8.name())
    state
  }

  def getMSC(hive: Hive): IMetaStoreClient = {
    getMSCMethod.invoke(hive).asInstanceOf[IMetaStoreClient]
  }

  private def toHiveDatabase(
                              database: CatalogDatabase, userName: Option[String] = None): HiveDatabase = {
    val props = database.properties
    val hiveDb = new HiveDatabase(
      database.name,
      database.description,
      CatalogUtils.URIToString(database.locationUri),
      (props -- Seq(PROP_OWNER)).asJava)
    props.get(PROP_OWNER).orElse(userName).foreach { ownerName =>
      hiveDb.setOwnerName(ownerName)
    }
    hiveDb.setCatalogName(catalogName)
    hiveDb
  }

  private def toInputFormat(name: String) =
    Utils.classForName[org.apache.hadoop.mapred.InputFormat[_, _]](name)

  private def toOutputFormat(name: String) =
    Utils.classForName[org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]](name)

  def toHiveTable(table: CatalogTable, userName: Option[String] = None): HiveTable = {
    val hiveTable = new HiveTable(table.database, table.identifier.table)
    hiveTable.setTableType(toHiveTableType(table.tableType))

    // For EXTERNAL_TABLE, we also need to set EXTERNAL field in the table properties.
    // Otherwise, Hive metastore will change the table to a MANAGED_TABLE.
    // (metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L1095-L1105)
    if (table.tableType == CatalogTableType.EXTERNAL) {
      hiveTable.setProperty("EXTERNAL", "TRUE")
    }
    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = table.schema.map(toHiveColumn).partition { c =>
      table.partitionColumnNames.contains(c.getName)
    }
    hiveTable.setFields(schema.asJava)
    hiveTable.setPartCols(partCols.asJava)
    Option(table.owner).filter(_.nonEmpty).orElse(userName).foreach(hiveTable.setOwner)
    hiveTable.setCreateTime(MILLISECONDS.toSeconds(table.createTime).toInt)
    hiveTable.setLastAccessTime(MILLISECONDS.toSeconds(table.lastAccessTime).toInt)
    table.storage.locationUri.map(CatalogUtils.URIToString).foreach { loc =>
      hiveTable.getTTable.getSd.setLocation(loc)
    }
    table.storage.inputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
    table.storage.outputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
    hiveTable.setSerializationLib(
      table.storage.serde.getOrElse("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    table.storage.properties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
    table.properties.foreach { case (k, v) => hiveTable.setProperty(k, v) }
    table.comment.foreach { c => hiveTable.setProperty("comment", c) }
    // Hive will expand the view text, so it needs 2 fields: viewOriginalText and viewExpandedText.
    // Since we don't expand the view text, but only add table properties, we map the `viewText` to
    // the both fields in hive table.
    table.viewText.foreach { t =>
      hiveTable.setViewOriginalText(t)
      hiveTable.setViewExpandedText(t)
    }
    hiveTable
  }

  def toHiveApiTable(ct: CatalogTable): MetaStoreApiTable ={
   // HiveTable.getEmptyTable(ct.database, ct.identifier.table)

    val apiTable = toHiveTable(ct, Some("hive")).getTTable
    apiTable.setCatName(catalogName)
    apiTable
  }
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val hiveDB = toHiveDatabase(dbDefinition,Some("extension"))
    msClient.createDatabase(hiveDB)

  }
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    msClient.dropDatabase(catalogName, db)
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val hivedb = toHiveDatabase(dbDefinition, Some("hive"))
    msClient.alterDatabase(dbDefinition.name, hivedb)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    val hiveDB = msClient.getDatabase(catalogName, db)
    CatalogDatabase(
      name = hiveDB.getName,
      description = Option(hiveDB.getDescription).getOrElse(""),
      locationUri = CatalogUtils.stringToURI(hiveDB.getLocationUri),
      properties = hiveDB.getParameters.asScala.toMap)
  }

  override def databaseExists(db: String): Boolean = {
    msClient.getDatabase(catalogName, db) != null
  }

  override def listDatabases(): Seq[String] = {
    msClient.getAllDatabases(catalogName).asScala.toSeq
  }

  override def listDatabases(pattern: String): Seq[String] = {
    listDatabases().filter(db => db.matches(pattern))
  }

  override def setCurrentDatabase(db: String): Unit = newState().setCurrentDatabase(db)

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    msClient.createTable(toHiveApiTable(tableDefinition))
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    msClient.dropTable(catalogName,db,table)
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    val ct = getTable(db, oldName)
    val newCt = ct.copy(identifier = TableIdentifier(newName, Some(db)))
    alterTable(newCt)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val convertedHiveTable = toHiveApiTable(tableDefinition)
    msClient.alter_table(catalogName, tableDefinition.database, tableDefinition.identifier.table,convertedHiveTable)
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    val ct = getTable(db, table)
    val newCt = ct.copy(schema = newDataSchema)
    alterTable(newCt)
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit ={
    val ct = getTable(db, table)
    val newCt = ct.copy(stats = stats)
    alterTable(newCt)
  }


  private def convertHiveTableToCatalogTable(h: HiveTable): CatalogTable = {
    // Note: Hive separates partition columns and the schema, but for us the
    // partition columns are part of the schema
    val (cols, partCols) = try {
      (h.getCols.asScala.map(fromHiveColumn), h.getPartCols.asScala.map(fromHiveColumn))
    } catch {
      case ex: SparkException =>
        throw QueryExecutionErrors.convertHiveTableToCatalogTableError(
          ex, h.getDbName, h.getTableName)
    }
    val schema = StructType((cols ++ partCols).toArray)

    val bucketSpec = if (h.getNumBuckets > 0) {
      val sortColumnOrders = h.getSortCols.asScala
      // Currently Spark only supports columns to be sorted in ascending order
      // but Hive can support both ascending and descending order. If all the columns
      // are sorted in ascending order, only then propagate the sortedness information
      // to downstream processing / optimizations in Spark
      // TODO: In future we can have Spark support columns sorted in descending order
      val allAscendingSorted = sortColumnOrders.forall(_.getOrder == HIVE_COLUMN_ORDER_ASC)

      val sortColumnNames = if (allAscendingSorted) {
        sortColumnOrders.map(_.getCol)
      } else {
        Seq.empty
      }
      Option(BucketSpec(h.getNumBuckets, h.getBucketCols.asScala.toSeq, sortColumnNames.toSeq))
    } else {
      None
    }

    // Skew spec and storage handler can't be mapped to CatalogTable (yet)
    val unsupportedFeatures = ArrayBuffer.empty[String]

    if (!h.getSkewedColNames.isEmpty) {
      unsupportedFeatures += "skewed columns"
    }

    if (h.getStorageHandler != null) {
      unsupportedFeatures += "storage handler"
    }

    if (h.getTableType == HiveTableType.VIRTUAL_VIEW && partCols.nonEmpty) {
      unsupportedFeatures += "partitioned view"
    }

    val properties = Option(h.getParameters).map(_.asScala.toMap).orNull

    // Hive-generated Statistics are also recorded in ignoredProperties
    val ignoredProperties = scala.collection.mutable.Map.empty[String, String]


    val excludedTableProperties = Set(
      // The property value of "comment" is moved to the dedicated field "comment"
      "comment",
      // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
      // in the function toHiveTable.
      "EXTERNAL"
    )

    val filteredProperties = properties.filterNot {
      case (key, _) => excludedTableProperties.contains(key)
    }
    val comment = properties.get("comment")

    CatalogTable(
      identifier = TableIdentifier(h.getTableName, Option(h.getDbName),Option(catalogName)),
      tableType = h.getTableType match {
        case HiveTableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
        case HiveTableType.MANAGED_TABLE => CatalogTableType.MANAGED
        case HiveTableType.VIRTUAL_VIEW => CatalogTableType.VIEW
        case unsupportedType =>
          val tableTypeStr = unsupportedType.toString.toLowerCase(Locale.ROOT).replace("_", " ")
          throw QueryCompilationErrors.hiveTableTypeUnsupportedError(h.getTableName, tableTypeStr)
      },
      schema = schema,
      partitionColumnNames = partCols.map(_.name).toSeq,
      // If the table is written by Spark, we will put bucketing information in table properties,
      // and will always overwrite the bucket spec in hive metastore by the bucketing information
      // in table properties. This means, if we have bucket spec in both hive metastore and
      // table properties, we will trust the one in table properties.
      bucketSpec = bucketSpec,
      owner = Option(h.getOwner).getOrElse(""),
      createTime = h.getTTable.getCreateTime.toLong * 1000,
      lastAccessTime = h.getLastAccessTime.toLong * 1000,
      storage = CatalogStorageFormat(
        locationUri = Some(CatalogUtils.stringToURI(h.getTTable.getSd.getLocation)),
        // To avoid ClassNotFound exception, we try our best to not get the format class, but get
        // the class name directly. However, for non-native tables, there is no interface to get
        // the format class name, so we may still throw ClassNotFound in this case.
        inputFormat = Option(h.getTTable.getSd.getInputFormat).orElse {
          Option(h.getStorageHandler).map(_.getInputFormatClass.getName)
        },
        outputFormat = Option(h.getTTable.getSd.getOutputFormat).orElse {
          Option(h.getStorageHandler).map(_.getOutputFormatClass.getName)
        },
        serde = Option(h.getSerializationLib),
        compressed = h.getTTable.getSd.isCompressed,
        properties = Option(h.getTTable.getSd.getSerdeInfo.getParameters)
          .map(_.asScala.toMap).orNull
      ),
      // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
      // in the function toHiveTable.
      properties = filteredProperties,
      //stats = readHiveStats(properties),
      comment = comment,
      // In older versions of Spark(before 2.2.0), we expand the view original text and
      // store that into `viewExpandedText`, that should be used in view resolution.
      // We get `viewExpandedText` as viewText, and also get `viewOriginalText` in order to
      // display the original view text in `DESC [EXTENDED|FORMATTED] table` command for views
      // that created by older versions of Spark.
      viewOriginalText = Option(h.getViewOriginalText),
      viewText = Option(h.getViewExpandedText),
      unsupportedFeatures = unsupportedFeatures.toSeq,
      ignoredProperties = ignoredProperties.toMap)
  }

  override def getTable(db: String, table: String): CatalogTable = {
    try {
      val ht = msClient.getTable(catalogName, db, table)
      convertHiveTableToCatalogTable(new org.apache.hadoop.hive.ql.metadata.Table(ht))
    }catch {
      case ex:NoSuchObjectException => null
      case e: Exception => throw e
    }
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    tables.map(tb => getTable(db, tb))
  }

  override def tableExists(db: String, table: String): Boolean = {
    msClient.tableExists(catalogName,db,table)
  }

  override def listTables(db: String): Seq[String] = {
    msClient.getAllTables(catalogName, db).asScala
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    listTables(db).filter(tb => tb.matches(pattern))
  }

  def toHiveTableType(catalogTableType: CatalogTableType): HiveTableType = {
    catalogTableType match {
      case CatalogTableType.EXTERNAL => HiveTableType.EXTERNAL_TABLE
      case CatalogTableType.MANAGED => HiveTableType.MANAGED_TABLE
      case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at toHiveTableType: $t")
    }
  }


  def toHiveAPITableType(catalogTableType: CatalogTableType): String = {
    catalogTableType match {
      case CatalogTableType.EXTERNAL => "EXTERNAL_TABLE"
      case CatalogTableType.MANAGED => "MANAGED_TABLE"
      case CatalogTableType.VIEW => "VIRTUAL_VIEW"
      case t =>
        throw new IllegalArgumentException(
          s"Unknown table type is found at toHiveTableType: $t")
    }
  }

  def toHivePartition(
                       p: CatalogTablePartition,
                       ht: HiveTable): HivePartition = {
    val tpart = new org.apache.hadoop.hive.metastore.api.Partition
    val partValues = ht.getPartCols.asScala.map { hc =>
      p.spec.getOrElse(hc.getName, throw new IllegalArgumentException(
        s"Partition spec is missing a value for column '${hc.getName}': ${p.spec}"))
    }
    val storageDesc = new StorageDescriptor
    val serdeInfo = new SerDeInfo
    p.storage.locationUri.map(CatalogUtils.URIToString(_)).foreach(storageDesc.setLocation)
    p.storage.inputFormat.foreach(storageDesc.setInputFormat)
    p.storage.outputFormat.foreach(storageDesc.setOutputFormat)
    p.storage.serde.foreach(serdeInfo.setSerializationLib)
    serdeInfo.setParameters(p.storage.properties.asJava)
    storageDesc.setSerdeInfo(serdeInfo)
    tpart.setDbName(ht.getDbName)
    tpart.setTableName(ht.getTableName)
    tpart.setValues(partValues.asJava)
    tpart.setSd(storageDesc)
    tpart.setCreateTime(MILLISECONDS.toSeconds(p.createTime).toInt)
    tpart.setLastAccessTime(MILLISECONDS.toSeconds(p.lastAccessTime).toInt)
    tpart.setParameters(mutable.Map(p.parameters.toSeq: _*).asJava)
    tpart.setCatName(catalogName)
    new HivePartition(ht, tpart)
  }

  /**
   * Build the native partition metadata from Hive's Partition.
   */
  def fromHivePartition(hp: HivePartition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition
    val properties: Map[String, String] = if (hp.getParameters != null) {
      hp.getParameters.asScala.toMap
    } else {
      Map.empty
    }
    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = CatalogStorageFormat(
        locationUri = Option(CatalogUtils.stringToURI(apiPartition.getSd.getLocation)),
        inputFormat = Option(apiPartition.getSd.getInputFormat),
        outputFormat = Option(apiPartition.getSd.getOutputFormat),
        serde = Option(apiPartition.getSd.getSerdeInfo.getSerializationLib),
        compressed = apiPartition.getSd.isCompressed,
        properties = Option(apiPartition.getSd.getSerdeInfo.getParameters)
          .map(_.asScala.toMap).orNull),
      createTime = apiPartition.getCreateTime.toLong * 1000,
      lastAccessTime = apiPartition.getLastAccessTime.toLong * 1000,
      parameters = properties,
      stats = readHiveStats(properties))
  }

  private def readHiveStats(properties: Map[String, String]): Option[CatalogStatistics] = {
    val totalSize = properties.get(StatsSetupConst.TOTAL_SIZE).filter(_.nonEmpty).map(BigInt(_))
    val rawDataSize = properties.get(StatsSetupConst.RAW_DATA_SIZE).filter(_.nonEmpty)
      .map(BigInt(_))
    val rowCount = properties.get(StatsSetupConst.ROW_COUNT).filter(_.nonEmpty).map(BigInt(_))
    // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
    // relatively cheap if parameters for the table are populated into the metastore.
    // Currently, only totalSize, rawDataSize, and rowCount are used to build the field `stats`
    // TODO: stats should include all the other two fields (`numFiles` and `numPartitions`).
    // (see StatsSetupConst in Hive)

    // When table is external, `totalSize` is always zero, which will influence join strategy.
    // So when `totalSize` is zero, use `rawDataSize` instead. When `rawDataSize` is also zero,
    // return None.
    // In Hive, when statistics gathering is disabled, `rawDataSize` and `numRows` is always
    // zero after INSERT command. So they are used here only if they are larger than zero.
    if (totalSize.isDefined && totalSize.get > 0L) {
      Some(CatalogStatistics(sizeInBytes = totalSize.get, rowCount = rowCount.filter(_ > 0)))
    } else if (rawDataSize.isDefined && rawDataSize.get > 0) {
      Some(CatalogStatistics(sizeInBytes = rawDataSize.get, rowCount = rowCount.filter(_ > 0)))
    } else {
      // TODO: still fill the rowCount even if sizeInBytes is empty. Might break anything?
      None
    }
  }


  override def listViews(db: String, pattern: String): Seq[String] = {
    val hiveTableType = toHiveTableType(CatalogTableType.VIEW)
    val tables = listTables(db)
    msClient.getTableObjectsByName(catalogName, db, tables.asJava)
      .asScala.
      filter(t => t.getTableType.equalsIgnoreCase(hiveTableType.toString)).map(t=> t.getTableName)

  }

  override protected def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  override protected def requireTableExists(db: String, table: String):Unit={
    if (!tableExists(db, table)) {
      throw new NoSuchTableException(db = db, table = table)
    }
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = ???


  // Partitions TBD
  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = {
    ???
  }

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = ???

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = {
    requireTableExists(db, table)

    val tableMeta = getTable(db, table)
    val ht = msClient.getTable(catalogName, db, table)
    val partitionColumnNames = tableMeta.partitionColumnNames
    val tablePath = new Path(tableMeta.location)
    val partsWithLocation = parts.map { p =>
      // Ideally we can leave the partition location empty and let Hive metastore to set it.
      // However, Hive metastore is not case preserving and will generate wrong partition location
      // with lower cased partition column names. Here we set the default partition location
      // manually to avoid this problem.
      val partitionPath = p.storage.locationUri.map(uri => new Path(uri)).getOrElse {
        ExternalCatalogUtils.generatePartitionPath(p.spec, partitionColumnNames, tablePath)
      }
      p.copy(storage = p.storage.copy(locationUri = Some(partitionPath.toUri)))
    }
    val metaStoreParts = partsWithLocation
      .map(p => p.copy(spec = toMetaStorePartitionSpec(p.spec)))
    val javaListOfPartitions = metaStoreParts.
      map(p => toHivePartition(p, new org.apache.hadoop.hive.ql.metadata.Table(ht))).
      map(p=> p.getTPartition).
      asJava
    msClient.add_partitions(javaListOfPartitions)
  }

  private def toMetaStorePartitionSpec(spec: TablePartitionSpec): TablePartitionSpec = {
    // scalastyle:off caselocale
    val lowNames = spec.map { case (k, v) => k.toLowerCase -> v }
    ExternalCatalogUtils.convertNullPartitionValues(lowNames)
    // scalastyle:on caselocale
  }

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = ???

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = ???

  override def alterPartitions(db: String, table: String, newParts: Seq[CatalogTablePartition]): Unit = {
    val metaStoreParts = newParts.map(p => p.copy(spec = toMetaStorePartitionSpec(p.spec)))
    // convert partition statistics to properties so that we can persist them through hive api
    val withStatsProps = metaStoreParts.map { p =>
      if (p.stats.isDefined) {
        val statsProperties = statsToProperties(p.stats.get)
        p.copy(parameters = p.parameters ++ statsProperties)
      } else {
        p
      }
    }
    val ht = new org.apache.hadoop.hive.ql.metadata.Table(msClient.getTable(catalogName,db,table))
    val hps = withStatsProps.map { toHivePartition( _,ht).getTPartition}.asJava
    msClient.alter_partitions(catalogName, db, table, hps)


  }

  private def statsToProperties(stats: CatalogStatistics): Map[String, String] = {

    val statsProperties = new mutable.HashMap[String, String]()
    statsProperties += STATISTICS_TOTAL_SIZE -> stats.sizeInBytes.toString()
    if (stats.rowCount.isDefined) {
      statsProperties += STATISTICS_NUM_ROWS -> stats.rowCount.get.toString()
    }

    stats.colStats.foreach { case (colName, colStat) =>
      colStat.toMap(colName).foreach { case (k, v) =>
        // Fully qualified name used in table properties for a particular column stat.
        // For example, for column "mycol", and "min" stat, this should return
        // "spark.sql.statistics.colStats.mycol.min".
        statsProperties += (STATISTICS_COL_STATS_PREFIX + k -> v)
      }
    }

    statsProperties.toMap
  }

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = {
    getPartitionOption(db,table, spec).getOrElse(None)
  }

  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    val ht = new org.apache.hadoop.hive.ql.metadata.Table(msClient.getTable(catalogName,db,table))
    var noPartition = false
    val jpartVals = ht.getPartCols.asScala.map(f => {

      spec.get(f.getName)

    }).filter(_.isDefined).map(_.get).asJava
    val hp = msClient.getPartition(catalogName, db, table, jpartVals)
    Some(fromHivePartition(new org.apache.hadoop.hive.ql.metadata.Partition(ht,hp)))
  }

  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    val catalogTable = getTable(db, table)
    val partColNameMap = buildLowerCasePartColNameMap(catalogTable).mapValues(escapePathName)
    val clientPartitionNames =  msClient.listPartitionNames(catalogName, db, table, -1).asScala

    clientPartitionNames.map { partitionPath =>
      val partSpec = PartitioningUtils.parsePathFragmentAsSeq(partitionPath)
      partSpec.map { case (partName, partValue) =>
        // scalastyle:off caselocale
        partColNameMap(partName.toLowerCase) + "=" + escapePathName(partValue)
        // scalastyle:on caselocale
      }.mkString("/")
    }

  }

  private def buildLowerCasePartColNameMap(table: CatalogTable): Map[String, String] = {
    val actualPartColNames = table.partitionColumnNames
    // scalastyle:off caselocale
    actualPartColNames.map(colName => (colName.toLowerCase, colName)).toMap
    // scalastyle:on caselocale
  }

  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    val hivePartitions = msClient.listPartitions(catalogName, db, table, -1).asScala
    val ht = new org.apache.hadoop.hive.ql.metadata.Table(msClient.getTable(catalogName,db,table))
    val catalogsPartitions = hivePartitions.map( hp => {
      val hiveMetadataPartition = new org.apache.hadoop.hive.ql.metadata.Partition(ht,hp)
      fromHivePartition(hiveMetadataPartition)
    }).toSeq
    catalogsPartitions
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    listPartitions(db,table)
  }


  // Function are not the major part for ExternalCatalog
  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def dropFunction(db: String, funcName: String): Unit = ???

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def renameFunction(db: String, oldName: String, newName: String): Unit = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = ???

  override def functionExists(db: String, funcName: String): Boolean = ???

  override def listFunctions(db: String, pattern: String): Seq[String] = ???
}

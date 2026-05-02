package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Hudi catalog for Spark 3.5.0 + Hudi 1.0.1.
 *
 * Register under any catalog name — NOT spark_catalog required:
 *   spark.sql.catalog.lake = com.example.hudi.multicatalog.catalog.MultiCatalogHoodieCatalog
 *   spark.sql.catalog.lake.warehouse = s3://bucket/warehouse
 *
 * Hudi 1.0.1 API changes vs 0.x:
 *  - HoodieTableMetaClient.builder().setConf() takes HadoopStorageConfiguration not Configuration
 *  - HoodieTableMetaClient.newTableBuilder().initTable() takes HadoopStorageConfiguration
 *  - Storage operations use HoodieStorage / HoodieStoragePath abstraction
 */
class MultiCatalogHoodieCatalog extends TableCatalog with SupportsNamespaces {

  private var catalogName: String               = _
  private var opts: CaseInsensitiveStringMap    = _
  private var warehousePath: String             = _
  private var hadoopConf: Configuration         = _
  private var spark: SparkSession               = _
  private var metastore: CatalogMetastore       = _

  // ─── Lifecycle ────────────────────────────────────────────────────────────

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName   = name
    this.opts          = options
    this.spark         = SparkSession.active
    this.hadoopConf    = spark.sessionState.newHadoopConf()
    this.warehousePath = options.getOrDefault(
      "warehouse",
      spark.conf.get("spark.sql.warehouse.dir", "/user/hive/warehouse")
    )
    this.metastore = buildMetastore(options)
  }

  override def name(): String = catalogName

  // ─── Namespace ────────────────────────────────────────────────────────────

  override def listNamespaces(): Array[Array[String]] =
    metastore.listDatabases().map(db => Array(db))

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    validateDepth(namespace)
    Array.empty
  }

  override def namespaceExists(namespace: Array[String]): Boolean =
    namespace.length == 1 && metastore.databaseExists(namespace(0))

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    validateDepth(namespace)
    if (!metastore.databaseExists(namespace(0))) throw new NoSuchNamespaceException(namespace)
    metastore.getDatabaseProperties(namespace(0)).asJava
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    validateDepth(namespace)
    metastore.createDatabase(namespace(0), namespacePath(namespace), metadata.asScala.toMap)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    validateDepth(namespace)
    val props = mutable.Map(metastore.getDatabaseProperties(namespace(0)).toSeq: _*)
    changes.foreach {
      case s: NamespaceChange.SetProperty    => props(s.property()) = s.value()
      case r: NamespaceChange.RemoveProperty => props.remove(r.property())
      case o => throw new UnsupportedOperationException(s"Unsupported namespace change: $o")
    }
    metastore.alterDatabase(namespace(0), props.toMap)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    validateDepth(namespace)
    if (!metastore.databaseExists(namespace(0))) return false
    if (!cascade && metastore.listTables(namespace(0)).nonEmpty)
      throw new IllegalStateException(s"Namespace ${namespace(0)} is not empty. Use CASCADE.")
    metastore.dropDatabase(namespace(0), cascade)
    true
  }

  // ─── Tables ───────────────────────────────────────────────────────────────

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    validateDepth(namespace)
    metastore.listTables(namespace(0)).map(t => Identifier.of(namespace, t))
  }

  override def tableExists(ident: Identifier): Boolean =
    metastore.tableExists(ident.namespace()(0), ident.name())

  /**
   * loadTable — returns MultiCatalogHudiTable which deliberately omits V2TableWithV1Fallback.
   * This keeps Spark on the DSv2 execution path for all operations (reads, writes, MERGE).
   */
  override def loadTable(ident: Identifier): Table = {
    val db  = ident.namespace()(0)
    val tbl = ident.name()
    if (!metastore.tableExists(db, tbl)) throw new NoSuchTableException(ident)
    val meta = metastore.getTableMetadata(db, tbl)
    val mc   = loadMetaClient(meta.location)
    new MultiCatalogHudiTable(spark, ident, mc, meta.properties, catalogName)
  }

  /**
   * Two-phase createTable:
   *   Phase 1 — Init Hudi timeline on storage (.hoodie/hoodie.properties)
   *   Phase 2 — Register in metastore
   *
   * Phase 1 MUST precede Phase 2 so concurrent readers who pick up the metastore
   * entry always find a valid timeline underneath.
   *
   * Hudi 1.0.1: initTable takes HadoopStorageConfiguration not raw hadoop Configuration.
   */
  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]
                          ): Table = {
    val db    = ident.namespace()(0)
    val tbl   = ident.name()
    if (metastore.tableExists(db, tbl)) throw new TableAlreadyExistsException(ident)

    val props       = properties.asScala
    val tableType   = resolveTableType(props)
    val basePath    = resolveBasePath(ident, props)
    val recordKey   = props.getOrElse(
      "hoodie.datasource.write.recordkey.field",
      throw new IllegalArgumentException("hoodie.datasource.write.recordkey.field is required")
    )
    val preCombine  = props.get("hoodie.datasource.write.precombine.field")
    val partFields  = partitions.map(_.describe()).mkString(",")

    // Phase 1: Hudi timeline init
    // Hudi 1.0.1: HadoopStorageConfiguration wraps hadoop Configuration
    val storageConf  = new HadoopStorageConfiguration(hadoopConf)
    val mcBuilder = HoodieTableMetaClient.newTableBuilder()
      .setTableType(tableType.name())
      .setTableName(tbl)
      .setRecordKeyFields(recordKey)
      .setPartitionFields(partFields)
      .setPayloadClassName(
        props.getOrElse(
          "hoodie.datasource.write.payload.class",
          "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        )
      )
    preCombine.foreach { v =>
      mcBuilder.set(Map("hoodie.datasource.write.precombine.field" -> v)
        .asInstanceOf[Map[String, Object]].asJava)
    }
    val metaClient = mcBuilder.initTable(storageConf, basePath)

    // Phase 2: metastore registration
    val finalProps = buildFinalProps(props.toMap, tableType, basePath, recordKey, partFields, schema)
    metastore.createTable(db, tbl, basePath, schema, finalProps)

    new MultiCatalogHudiTable(spark, ident, metaClient, finalProps, catalogName)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val db  = ident.namespace()(0)
    val tbl = ident.name()
    if (!metastore.tableExists(db, tbl)) throw new NoSuchTableException(ident)
    val meta      = metastore.getTableMetadata(db, tbl)
    val newSchema = applySchemaChanges(meta.schema, changes)
    metastore.alterTableSchema(db, tbl, newSchema)
    new MultiCatalogHudiTable(spark, ident, loadMetaClient(meta.location), meta.properties, catalogName)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val db  = ident.namespace()(0)
    val tbl = ident.name()
    if (!metastore.tableExists(db, tbl)) return false
    val meta  = metastore.getTableMetadata(db, tbl)
    val purge = meta.properties.getOrElse("purge.on.drop", "false").toBoolean
    if (purge) {
      // Hudi 1.0.1 storage abstraction
      val storagePath = new StoragePath(meta.location)
      val storage     = new HoodieHadoopStorage(storagePath, new HadoopStorageConfiguration(hadoopConf))
      storage.deleteDirectory(storagePath)
    }
    metastore.dropTable(db, tbl)
    true
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException(
      "Hudi tables cannot be renamed — base path is embedded in hoodie.properties"
    )

  // ─── Internal helpers ─────────────────────────────────────────────────────

  /**
   * Hudi 1.0.1: setConf takes HadoopStorageConfiguration not raw Configuration.
   */
  private def loadMetaClient(basePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder()
      .setConf(new HadoopStorageConfiguration(hadoopConf))
      .setBasePath(basePath)                  // takes String — confirmed from source
      .setLoadActiveTimelineOnLoad(true)
      .build()

  private def resolveTableType(props: mutable.Map[String, String]): HoodieTableType =
    HoodieTableType.valueOf(
      props.getOrElse(HoodieTableConfig.TYPE.key(), "COPY_ON_WRITE").toUpperCase
    )

  private def resolveBasePath(ident: Identifier, props: mutable.Map[String, String]): String =
    props.getOrElse("location",
      s"$warehousePath/${ident.namespace()(0)}.db/${ident.name()}")

  private def namespacePath(ns: Array[String]): String = s"$warehousePath/${ns(0)}.db"

  private def validateDepth(namespace: Array[String]): Unit =
    if (namespace.length != 1)
      throw new IllegalArgumentException(
        s"Only single-level namespaces are supported, got: ${namespace.mkString(".")}"
      )

  private def buildFinalProps(
                               base: Map[String, String],
                               tableType: HoodieTableType,
                               basePath: String,
                               recordKey: String,
                               partitionFields: String,
                               schema: StructType
                             ): Map[String, String] = base ++ Map(
    "provider"                                    -> "hudi",
    "location"                                    -> basePath,
    HoodieTableConfig.TYPE.key()                  -> tableType.name(),
    "hoodie.datasource.write.recordkey.field"     -> recordKey,
    "hoodie.datasource.write.partitionpath.field" -> partitionFields,
    "spark.sql.sources.schema"                    -> schema.json
  )

  private def applySchemaChanges(current: StructType, changes: Seq[TableChange]): StructType = {
    var schema = current
    changes.foreach {
      case a: AddColumn =>
        schema = StructType(schema.fields :+
          StructField(a.fieldNames().last, a.dataType(), a.isNullable))
      case r: RenameColumn =>
        schema = StructType(schema.fields.map(f =>
          if (f.name == r.fieldNames().last) f.copy(name = r.newName()) else f))
      case d: DeleteColumn =>
        schema = StructType(schema.fields.filterNot(_.name == d.fieldNames().last))
      case u: UpdateColumnType =>
        schema = StructType(schema.fields.map(f =>
          if (f.name == u.fieldNames().last) f.copy(dataType = u.newDataType()) else f))
      case n: UpdateColumnNullability =>
        schema = StructType(schema.fields.map(f =>
          if (f.name == n.fieldNames().last) f.copy(nullable = n.nullable()) else f))
      case o => throw new UnsupportedOperationException(s"Unsupported schema change: $o")
    }
    schema
  }

  private def buildMetastore(options: CaseInsensitiveStringMap): CatalogMetastore = {
    val cls = options.getOrDefault(
      "metastore.backend",
      "com.example.hudi.multicatalog.catalog.InMemoryCatalogMetastore"
    )
    Class.forName(cls)
      .getDeclaredConstructor(classOf[CaseInsensitiveStringMap])
      .newInstance(options)
      .asInstanceOf[CatalogMetastore]
  }
}
package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi



import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * Core DSv2 Table for multi-catalog Hudi.
 * Targets Spark 3.5.0 + Hudi 1.0.1.
 *
 * Interface decisions:
 *
 *  SupportsRowLevelOperations  — Spark 3.5 interface in org.apache.spark.sql.connector.catalog.
 *    Causes ResolveRowLevelCommands to fire natively for MERGE/UPDATE/DELETE on any catalog.
 *    No custom MergeIntoTable rewrite rule is needed.
 *
 *  NO V2TableWithV1Fallback    — Omitting this prevents DataSourceV2Strategy from
 *    short-circuiting to DSv1 RowDataSourceScanExec. All paths stay on DSv2.
 *
 *  SupportsRead                — Wired to HudiScanBuilder (COW + MOR DSv2 scan).
 *  SupportsWrite               — Wired to HudiWriteBuilder (INSERT / OVERWRITE).
 *
 * AvroConversionUtils in Hudi 1.0.1:
 *  convertAvroSchemaToStructType(Schema): StructType
 *  convertStructTypeToAvroSchema(StructType, name, namespace): Schema
 */
class MultiCatalogHudiTable(
                             val spark: SparkSession,
                             val ident: Identifier,
                             val metaClient: HoodieTableMetaClient,
                             val tableProps: Map[String, String],
                             val catalogName: String
                           ) extends Table
  with SupportsRead
  with SupportsWrite
  with SupportsRowLevelOperations  // Spark 3.5: org.apache.spark.sql.connector.catalog
 {

  lazy val tableType: HoodieTableType =
    metaClient.getTableConfig.getTableType

  /**
   * User-visible schema — excludes Hudi metadata fields (_hoodie_commit_time etc.).
   * Hudi 1.0.1: TableSchemaResolver.getTableAvroSchemaWithoutMetadataFields()
   */
  lazy val tableSchema: StructType = {
    val resolver = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(
      resolver.getTableAvroSchemaWithoutMetadataFields
    )
  }

  /**
   * Full schema including _hoodie_* meta fields.
   * Required by MOR merge reader and SupportsRowLevelOperations write path.
   * Hudi 1.0.1: getTableAvroSchema(false) — false = include metadata fields.
   */
  lazy val tableSchemaWithMeta: StructType = {
    val resolver = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(
      resolver.getTableAvroSchema(false)
    )
  }

  // ─── Table identity ────────────────────────────────────────────────────────

  override def name(): String =
    s"$catalogName.${ident.namespace().mkString(".")}.${ident.name()}"

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] = {
    val fields = metaClient.getTableConfig.getPartitionFieldProp
    if (fields == null || fields.isEmpty) Array.empty
    else fields.split(",").map { f =>
      org.apache.spark.sql.connector.expressions.Expressions.identity(f.trim)
    }
  }

  override def properties(): util.Map[String, String] = {
    val p = new util.HashMap[String, String]()
    tableProps.foreach { case (k, v) => p.put(k, v) }
    p.put(TableCatalog.PROP_PROVIDER, "hudi")
    p.put(TableCatalog.PROP_LOCATION, metaClient.getBasePath.toString)
    p.put("hudi.table.type", tableType.name())
    p
  }

  override def capabilities(): util.Set[TableCapability] = {
    import TableCapability._
    // ROW_LEVEL_OPERATION_CHECK_REFERENCES — tells Spark to include metadata attributes
    // in the MERGE target scan so requiredMetadataAttributes() can propagate
    util.EnumSet.of(
      BATCH_READ,
      BATCH_WRITE,
      OVERWRITE_BY_FILTER,
      OVERWRITE_DYNAMIC,
      TRUNCATE
    )
  }

  // ─── Read ──────────────────────────────────────────────────────────────────

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val merged = new util.HashMap[String, String]()
    tableProps.foreach { case (k, v) => merged.put(k, v) }
    options.forEach { case (k, v) => merged.put(k, v) }
    new HudiScanBuilder(
      spark       = spark,
      metaClient  = metaClient,
      tableType   = tableType,
      schema      = tableSchema,
      schemaMeta  = tableSchemaWithMeta,
      options     = new CaseInsensitiveStringMap(merged)
    )
  }

  // ─── Write (INSERT INTO / INSERT OVERWRITE) ────────────────────────────────

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new HudiWriteBuilder(spark, metaClient, tableType, tableProps, info)

  // ─── Row-level operations (MERGE / UPDATE / DELETE) ────────────────────────
  //
  // Spark 3.5.0:
  //   SupportsRowLevelOperations is in org.apache.spark.sql.connector.catalog
  //   RowLevelOperationBuilder   is in org.apache.spark.sql.connector.catalog
  //   RowLevelOperationInfo      is in org.apache.spark.sql.connector.write
  //
  // ResolveRowLevelCommands sees this implementation and rewrites:
  //   MERGE  → ReplaceData (COW) or WriteDelta (MOR)
  //   UPDATE → ReplaceData (COW) or WriteDelta (MOR)
  //   DELETE → ReplaceData (COW) or WriteDelta (MOR)
  //
  // These are optimizer-visible logical nodes — AQE, CBO, and predicate pushdown all apply.

  override def newRowLevelOperationBuilder(info: RowLevelOperationInfo): RowLevelOperationBuilder =
    new HudiRowLevelOperationBuilder(spark, metaClient, tableType, tableProps, info)

  // ─── Partition management ──────────────────────────────────────────────────

//  override def createPartition(ident: InternalRow, props: util.Map[String, String]): Unit = {
//    // Hudi creates partitions implicitly on write — no explicit registration needed
//  }

//  neededoverride def dropPartition(ident: InternalRow): Boolean =
//    throw new UnsupportedOperationException(
//      "Use CALL system.drop_partition() for Hudi partition deletion"
//    )

   def replacePartitionMetadata(ident: InternalRow, props: util.Map[String, String]): Unit = {}

   def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] =
    util.Collections.emptyMap()

   def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] =
    Array.empty
}
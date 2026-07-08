package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi


import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRowLevelOperations}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import scala.collection.JavaConverters._

/**
 * RowLevelOperationBuilder for Spark 3.5.0 + Hudi 1.0.1.
 *
 * Spark 3.5 package locations (important — differs from earlier Spark versions):
 *   SupportsRowLevelOperations  → org.apache.spark.sql.connector.catalog
 *   RowLevelOperationBuilder    → org.apache.spark.sql.connector.catalog
 *   RowLevelOperation           → org.apache.spark.sql.connector.write
 *   RowLevelOperationInfo       → org.apache.spark.sql.connector.write
 *   RowLevelOperation.Command   → org.apache.spark.sql.connector.write.RowLevelOperation.Command
 *   FieldReference              → org.apache.spark.sql.connector.expressions.FieldReference
 *
 * Called by Spark's ResolveRowLevelCommands for MERGE/UPDATE/DELETE.
 * Because MultiCatalogHudiTable implements SupportsRowLevelOperations, this builder
 * fires for tables in ANY catalog — not just spark_catalog.
 */
class HudiRowLevelOperationBuilder(
                                    spark: SparkSession,
                                    metaClient: HoodieTableMetaClient,
                                    tableType: HoodieTableType,
                                    tableProps: Map[String, String],
                                    catalogName: String,
                                    info: RowLevelOperationInfo
                                  ) extends RowLevelOperationBuilder {   // org.apache.spark.sql.connector.catalog

  override def build(): RowLevelOperation = info.command() match {
    case Command.MERGE  => new HudiMergeOperation(spark, metaClient, tableType, tableProps, catalogName)
    case Command.UPDATE => new HudiUpdateOperation(spark, metaClient, tableType, tableProps, catalogName)
    case Command.DELETE => new HudiDeleteOperation(spark, metaClient, tableType, tableProps, catalogName)
    case other          => throw new UnsupportedOperationException(s"Unsupported command: $other")
  }
}

// ─── Base operation ───────────────────────────────────────────────────────────

/**
 * Base for MERGE / UPDATE / DELETE operations.
 *
 * newScanBuilder — used by Spark to scan the MERGE target table.
 *   Must expose schemaMeta (including _hoodie_* columns) so that
 *   requiredMetadataAttributes can be propagated through the plan.
 *
 * requiredMetadataAttributes — tells Spark to preserve Hudi meta columns
 *   through the MERGE join so the write side can:
 *     _hoodie_record_key      → upsert/delete key
 *     _hoodie_partition_path  → route rows to correct partition
 *     _hoodie_file_name       → identify file group for COW rewrite / MOR log append
 *     _hoodie_commit_time     → ordering / precombine context
 *
 * Hudi 1.0.1: TableSchemaResolver.getTableAvroSchemaWithoutMetadataFields()
 *             and getTableAvroSchema(false) are unchanged from 0.x.
 */
abstract class HudiBaseRowLevelOperation(
                                          spark: SparkSession,
                                          metaClient: HoodieTableMetaClient,
                                          tableType: HoodieTableType,
                                          tableProps: Map[String, String],
                                          catalogName: String
                                        ) extends RowLevelOperation {

  protected lazy val tableSchema: StructType = {
    val resolver = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(
      resolver.getTableAvroSchemaWithoutMetadataFields
    )
  }

  protected lazy val tableSchemaWithMeta: StructType = {
    val resolver = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(
      resolver.getTableAvroSchema(false)
    )
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val merged = new ju.HashMap[String, String]()
    tableProps.foreach { case (k, v) => merged.put(k, v) }
    options.forEach { case (k, v) => merged.put(k, v) }
    new HudiScanBuilder(
      spark      = spark,
      metaClient = metaClient,
      tableType  = tableType,
      schema     = tableSchemaWithMeta,  // expose meta fields for row-level op target scan
      schemaMeta = tableSchemaWithMeta,
      options    = new CaseInsensitiveStringMap(merged)
    )
  }

  /**
   * Hudi meta columns that must flow from target scan → write side.
   *
   * Spark 3.5.0: FieldReference.column(name) is in
   *   org.apache.spark.sql.connector.expressions.FieldReference
   *
   * Hudi 1.0.1 meta field names are unchanged from 0.x.
   */
  override def requiredMetadataAttributes(): Array[NamedReference] = Array(
    FieldReference.column("_hoodie_record_key"),
    FieldReference.column("_hoodie_partition_path"),
    FieldReference.column("_hoodie_file_name"),
    FieldReference.column("_hoodie_commit_time")
  )
}

// ─── MERGE ────────────────────────────────────────────────────────────────────

/**
 * MERGE INTO target USING source ON condition
 *   WHEN MATCHED     THEN UPDATE SET ...
 *   WHEN NOT MATCHED THEN INSERT  ...
 *
 * Spark 3.5 rewrites to:
 *   COW → ReplaceData   (full file group rewrite — aligns with COW semantics)
 *   MOR → WriteDelta    (delta rows only      — aligns with MOR log-append semantics)
 */
class HudiMergeOperation(
                          spark: SparkSession,
                          metaClient: HoodieTableMetaClient,
                          tableType: HoodieTableType,
                          tableProps: Map[String, String],
                          catalogName: String
                        ) extends HudiBaseRowLevelOperation(spark, metaClient, tableType, tableProps, catalogName) {

  override def command(): Command = Command.MERGE

  // SparkRDDWriteClient.upsert()/.delete() already dispatch COW vs MOR internally by reading
  // metaClient's table type — no need to pick a builder class based on tableType here.
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new HudiRowLevelWriteBuilder(spark, metaClient, tableType, tableProps, catalogName, info)
}

// ─── UPDATE ───────────────────────────────────────────────────────────────────

/**
 * UPDATE target SET col = expr WHERE condition
 * Semantically equivalent to MERGE with only a MATCHED/UPDATE clause.
 */
class HudiUpdateOperation(
                           spark: SparkSession,
                           metaClient: HoodieTableMetaClient,
                           tableType: HoodieTableType,
                           tableProps: Map[String, String],
                           catalogName: String
                         ) extends HudiBaseRowLevelOperation(spark, metaClient, tableType, tableProps, catalogName) {

  override def command(): Command = Command.UPDATE

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new HudiRowLevelWriteBuilder(spark, metaClient, tableType, tableProps, catalogName, info)
}

// ─── DELETE ───────────────────────────────────────────────────────────────────

/**
 * DELETE FROM target WHERE condition
 *   COW → ReplaceData: file groups rewritten with matching rows removed
 *   MOR → WriteDelta:  DELETE_BLOCK appended to log; compaction collapses later
 */
class HudiDeleteOperation(
                           spark: SparkSession,
                           metaClient: HoodieTableMetaClient,
                           tableType: HoodieTableType,
                           tableProps: Map[String, String],
                           catalogName: String
                         ) extends HudiBaseRowLevelOperation(spark, metaClient, tableType, tableProps, catalogName) {

  override def command(): Command = Command.DELETE

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new HudiRowLevelWriteBuilder(spark, metaClient, tableType, tableProps, catalogName, info)
}
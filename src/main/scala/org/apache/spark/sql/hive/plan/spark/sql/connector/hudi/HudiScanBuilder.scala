package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * ScanBuilder for COW and MOR tables.
 * Spark 3.5.0 + Hudi 1.0.1 compatible.
 *
 * SupportsPushDownFilters     — partition pruning via HoodieFileIndex
 * SupportsPushDownRequiredColumns — column projection to reduce Parquet I/O
 *
 * Filter split strategy:
 *  Partition filters → pushed into HoodieFileIndex.listFiles() → fewer file splits
 *  Data filters      → returned as unhandled → Spark applies post-scan
 *
 * HoodieFileIndex in Hudi 1.0.1:
 *   Constructor: HoodieFileIndex(spark, metaClient, schemaSpec, options, fileStatusCache)
 *   options must be Map[String, String] (case-sensitive)
 *   FileStatusCache.getOrCreate(spark) from org.apache.spark.sql.execution.datasources
 */
class HudiScanBuilder(
                       spark: SparkSession,
                       metaClient: HoodieTableMetaClient,
                       tableType: HoodieTableType,
                       schema: StructType,       // user-visible schema (no _hoodie_* fields)
                       schemaMeta: StructType,   // schema with _hoodie_* meta fields
                       options: CaseInsensitiveStringMap
                     ) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  private var pushedPartFilters: Array[Filter] = Array.empty
  private var unpushedFilters: Array[Filter]   = Array.empty
  private var requiredSchema: StructType        = schema

  // Partition columns declared in the table config
  private lazy val partitionCols: Set[String] = {
    val fields = metaClient.getTableConfig.getPartitionFieldProp
    if (fields == null || fields.isEmpty) Set.empty[String]
    else fields.split(",").map(_.trim.toLowerCase).toSet
  }

  // ─── SupportsPushDownFilters ───────────────────────────────────────────────

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // Previously classified purely by column reference: `extractColumns(f).forall(partitionCols
    // .contains)`. Any filter passing that check got marked fully-handled (removed from what's
    // returned to Spark for post-scan reapplication) — but HudiFilterConverter.toCatalyst
    // couldn't convert every Filter shape (e.g. StringStartsWith/Contains/EndsWith), and used to
    // silently degrade those to "always true" for pruning. Since Spark had already been told
    // the filter was handled, that meant rows outside the real predicate leaked through with no
    // correctness check anywhere. Now also requires HudiFilterConverter.isConvertible(f) before
    // treating a filter as handled — anything that fails that check is routed to dataFilters
    // instead, so Spark still enforces it as a residual filter (just not used for pruning).
    val (partFilters, dataFilters) = filters.partition { f =>
      extractColumns(f).forall(c => partitionCols.contains(c.toLowerCase)) &&
        HudiFilterConverter.isConvertible(f)
    }
    pushedPartFilters = partFilters
    unpushedFilters   = dataFilters
    dataFilters  // return to Spark for post-scan application
  }

  override def pushedFilters(): Array[Filter] = pushedPartFilters

  // ─── SupportsPushDownRequiredColumns ──────────────────────────────────────

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  // ─── Build ────────────────────────────────────────────────────────────────

  override def build(): Scan = {
    val fileIndex = buildFileIndex()
    tableType match {
      case HoodieTableType.COPY_ON_WRITE =>
        new HudiCOWScan(spark, metaClient, fileIndex, requiredSchema, pushedPartFilters, options)
      case HoodieTableType.MERGE_ON_READ =>
        new HudiMORScan(spark, metaClient, fileIndex, requiredSchema, schemaMeta, pushedPartFilters, options)
    }
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Hudi 1.0.1: HoodieFileIndex constructor takes Map[String, String] for options.
   * FileStatusCache from org.apache.spark.sql.execution.datasources.
   */
  private def buildFileIndex(): HoodieFileIndex = {
    val indexOptions = options.asCaseSensitiveMap().asScala.toMap ++ Map(
      "path"                    -> metaClient.getBasePath.toString,
      "hoodie.metadata.enable"  -> options.getOrDefault("hoodie.metadata.enable", "true")
    )
    new HoodieFileIndex(
      spark,
      metaClient,
      Some(requiredSchema),
      indexOptions,
      FileStatusCache.getOrCreate(spark)
    )
  }

  /**
   * Extract all column references from a DSv2 Filter.
   * Conservative: any unrecognised filter type is treated as a data filter (not partition-safe).
   */
  private def extractColumns(filter: Filter): Set[String] = filter match {
    case EqualTo(col, _)            => Set(col)
    case EqualNullSafe(col, _)      => Set(col)
    case GreaterThan(col, _)        => Set(col)
    case GreaterThanOrEqual(col, _) => Set(col)
    case LessThan(col, _)           => Set(col)
    case LessThanOrEqual(col, _)    => Set(col)
    case In(col, _)                 => Set(col)
    case IsNull(col)                => Set(col)
    case IsNotNull(col)             => Set(col)
    case StringStartsWith(col, _)   => Set(col)
    case StringEndsWith(col, _)     => Set(col)
    case StringContains(col, _)     => Set(col)
    case And(l, r)                  => extractColumns(l) ++ extractColumns(r)
    case Or(l, r)                   => extractColumns(l) ++ extractColumns(r)
    case Not(child)                 => extractColumns(child)
    case _                          => Set("__unrecognised__") // force data-filter treatment
  }
}
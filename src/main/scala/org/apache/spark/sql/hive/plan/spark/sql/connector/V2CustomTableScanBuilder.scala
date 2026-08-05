package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScanBuilder
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScanBuilder
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
import org.apache.spark.sql.execution.datasources.v2.text.TextScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScanBuilder

import scala.collection.JavaConverters.mapAsScalaMapConverter

// ─────────────────────────────────────────────────────────────────────────────
// CHANGE LIST
// ─────────────────────────────────────────────────────────────────────────────
//
// FIX 1 — pushDataFilters: throwaway instances replaced by a persistent delegate
//   BEFORE: each call to pushDataFilters created a brand-new format-specific
//           ScanBuilder, called pushDataFilters on it, and immediately discarded
//           it. The pushed-filter state was lost. FileScanBuilder's own
//           pushedDataFilters field (which build() reads) was never populated.
//           Result: V2CustomTableScan was built with zero pushed filters, so
//           no predicate pushdown into Parquet row groups / ORC bloom filters
//           ever happened — every file was fully scanned regardless of WHERE clause.
//           This is the single biggest TPC-DS regression vs open-source tables.
//   AFTER:  one persistent `formatScanBuilder` held as a field. pushDataFilters,
//           pushAggregation, and build() all operate on the same stateful instance.
//           Accepted filters accumulate correctly and flow into the scan.
//
// FIX 2 — pushAggregation: delegated to formatScanBuilder per format
//   BEFORE: used parquetAggregatePushDown conf as the gate for ORC too (wrong).
//           Also computed finalSchema independently of the builder that knows
//           which columns the aggregation actually requires.
//   AFTER:  parquet and orc delegate to their own ScanBuilder instances via the
//           SupportsPushDownAggregates interface. Each format gates on its own
//           correct conf. Schema and pushed state come from the same builder used
//           everywhere else — no dual tracking.
//
// FIX 3 — catalogTable added as constructor field
//   Threaded through to V2CustomTableScan so estimateStatistics() can expose
//   ANALYZE TABLE row counts to CBO (see FIX 6 in V2CustomTableScan).
//
// FIX 4 — stale imports removed
//   RebaseDateTime.RebaseSpec, ParquetFilters, SparkToParquetSchemaConverter,
//   LegacyBehaviorPolicy, ParquetScan — imported but never used.
// ─────────────────────────────────────────────────────────────────────────────

case class V2CustomTableScanBuilder(
                                     multiPartName: Seq[String],
                                     format: String,
                                     sparkSession: SparkSession,
                                     fileIndex: PartitioningAwareFileIndex,
                                     schema: StructType,
                                     dataSchema: StructType,
                                     options: CaseInsensitiveStringMap,
                                     // FIX 3 — needed for CBO stats exposure
                                     catalogTable: Option[CatalogTable] = None)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates {

  // FIX 1 — one persistent builder per format; all pushXxx calls go here
  private val formatScanBuilder: FileScanBuilder = format.toLowerCase match {
    case "parquet"  => ParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "orc"      => OrcScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "csv"      => CSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "json"     => JsonScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "avro"     => AvroScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "text"     => TextScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
    case "textfile" => CSVScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var finalSchema       = new StructType()
  private var pushedAggregations = Option.empty[Aggregation]

  override protected val supportsNestedSchemaPruning: Boolean = true

  // FIX 1 — delegate to persistent builder so state is retained across calls
  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] =
    formatScanBuilder.pushDataFilters(dataFilters)

  // FIX 2 — each format delegates to its own builder with its own correct conf
  override def pushAggregation(aggregation: Aggregation): Boolean =
    format.toLowerCase match {
      case "csv" | "json" | "avro" | "text" | "textfile" =>
        false

      case "parquet" | "orc" =>
        formatScanBuilder match {
          case agg: SupportsPushDownAggregates =>
            val pushed = agg.pushAggregation(aggregation)
            if (pushed) {
              pushedAggregations = Some(aggregation)
              finalSchema = formatScanBuilder.build().readSchema()
            }
            pushed
          case _ => false
        }

      case _ => false
    }

  override def build(): V2CustomTableScan = {
    // FIX 1 — read pushed filters from the builder that actually accumulated them
    val pushed = formatScanBuilder.pushedDataFilters

    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }

    V2CustomTableScan(
      multiPartName, format, sparkSession, hadoopConf, fileIndex, dataSchema,
      finalSchema, readPartitionSchema(), pushed, options, pushedAggregations,
      partitionFilters, dataFilters,
      catalogTable   // FIX 3
    )
  }
}
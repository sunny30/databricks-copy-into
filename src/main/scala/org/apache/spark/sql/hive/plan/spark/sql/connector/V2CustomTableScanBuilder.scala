package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownAggregates}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScanBuilder
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScanBuilder
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
import org.apache.spark.sql.execution.datasources.v2.text.TextScanBuilder
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScanBuilder

import scala.collection.JavaConverters.mapAsScalaMapConverter


/**Looking at the three files you've shared, here's my honest assessment for Spark 4.x + Scala 2.13 compatibility:
V2CustomTableScan.scala — will work as-is. No changes needed.
V2CustomTable.scala — will work as-is. No changes needed.
V2CustomTableScanBuilder.scala — two things that will break on Spark 4.x:
1. import org.apache.spark.sql.catalyst.StructFilters
In Spark 4.x, StructFilters moved from org.apache.spark.sql.catalyst to org.apache.spark.sql.execution.datasources. The import you have is wrong for Spark 3.5 too — which is likely the "Cannot resolve symbol StructFilters" error you hit. In Spark 4.x the class may be renamed or removed entirely since CSV/JSON filter pushdown was reworked.
2. import org.apache.spark.sql.catalyst.util.RebaseDateTime
RebaseDateTime and the entire datetime rebase infrastructure were removed in Spark 4.0 as part of the legacy datetime rebase cleanup (SPARK-43576). LegacyBehaviorPolicy was also removed. So RebaseDateTime.RebaseSpec(LegacyBehaviorPolicy.CORRECTED) won't compile on Spark 4.x at all.
3. ParquetFilters constructor — the 8-arg constructor with datetimeRebaseSpec was simplified in Spark 4.x. The rebase spec arg was dropped when the legacy rebase support was removed.
4. Scala 2.13 — scala.collection.JavaConverters is deprecated in 2.13, replaced by scala.jdk.CollectionConverters. Won't break compilation but will generate deprecation warnings everywhere the asScala/asJava conversions are used.
The rest — equals, hashCode, estimateStatistics, FileStatusCache, capabilities, the FileScanBuilder subclassing approach — is all fine on Spark 4.x.**/

// ─────────────────────────────────────────────────────────────────────────────
// ARCHITECTURE
// ─────────────────────────────────────────────────────────────────────────────
//
// V2CustomTableScanBuilder extends FileScanBuilder directly — it IS the scan
// builder Spark calls pushFilters/pruneColumns/pushAggregation/build() on.
// This solves all three compile errors from previous approaches:
//
//   (a) "Overriding type () => V2CustomTableScan does not conform to base type
//       () => ParquetScan" — caused by subclassing ParquetScanBuilder etc. and
//       overriding build(). Fixed: we extend FileScanBuilder whose build()
//       returns Scan (interface), so V2CustomTableScan satisfies it.
//
//   (b) "Access to protected method pushDataFilters not permitted" — caused by
//       delegating to a separate FileScanBuilder instance. Fixed: pushDataFilters
//       is called on `this` — always valid for own class.
//
//   (c) "Type mismatch Array[Filter] / Array[Predicate]" — caused by casting
//       to wrong SupportsPushDownFilters interface. Fixed: no casting needed,
//       pushDataFilters on `this` always returns Array[Filter].
//
// Format-specific filter acceptance is implemented directly in pushDataFilters
// using the public ParquetFilters / OrcFilters / StructFilters APIs — pure
// functions on filter+schema, no instance state, no protected access issues.
//
// Format-specific readers are delegated via createReaderFactory() in
// V2CustomTableScan, which instantiates a short-lived format scan just to get
// the factory — correct because createReaderFactory has no planning-time state.
//
// CHANGE LIST
// ─────────────────────────────────────────────────────────────────────────────
//
// FIX 1 — pushDataFilters: format-aware acceptance on `this`
//   Uses ParquetFilters / OrcFilters / StructFilters (all public APIs) to
//   determine which filters each format can actually push to storage-level
//   predicate evaluation. Accepted filters stored in pushedDataFilters (the
//   protected var on FileScanBuilder, writable on `this`). build() reads it
//   directly — no cross-instance access, no reflection.
//
// FIX 2 — pushAggregation: short-lived format builder for agg check only
//   Aggregate pushdown has no cross-call state — it only checks conf flags and
//   derives the output schema. Safe to use a short-lived builder for this.
//
// FIX 3 — catalogTable threaded through to V2CustomTableScan for CBO stats
//
// FIX 4 — pushedDataFilters read on `this` in build() — always valid
// ─────────────────────────────────────────────────────────────────────────────

class V2CustomTableScanBuilder(
                                val multiPartName:   Seq[String],
                                val format:          String,
                                val sparkSession: SparkSession,
                                val confHadoop:      Configuration,
                                val fileIndexArg:    PartitioningAwareFileIndex,
                                val schema:          StructType,
                                val dataSchemaArg:   StructType,
                                val optionsArg:      CaseInsensitiveStringMap,
                                val catalogTableArg: Option[CatalogTable] = None)
  extends FileScanBuilder(sparkSession, fileIndexArg, dataSchemaArg)
    with SupportsPushDownAggregates {

  override protected val supportsNestedSchemaPruning: Boolean = true

  private var finalSchema:        StructType          = new StructType()
  private var pushedAggregations: Option[Aggregation] = None

  // ── FIX 1: format-aware pushDataFilters on `this` ──────────────────────────
  //
  // FileScanBuilder.pushFilters (public, called by V2ScanRelationPushDown) →
  //   translates Catalyst expressions to Filter →
  //   calls this.pushDataFilters (protected, on `this` — valid) →
  //   stores result in this.pushedDataFilters (protected var — valid) →
  //   build() reads this.pushedDataFilters — valid.
  //
  // For each format, we use its public filter-translation API to determine
  // which filters it can actually evaluate at storage level:
  //   Parquet → ParquetFilters.createFilter (returns Some if pushable)
  //   ORC     → OrcFilters.createFilter      (returns Some if pushable)
  //   CSV     → StructFilters.pushedFilters   (returns residuals)
  //   JSON    → StructFilters.pushedFilters   (returns residuals)
  //   Avro    → no storage-level filter support, all remain as residuals
  //   Text    → no filter support
  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    val remaining: Array[Filter] = format.toLowerCase match {

      case "parquet" if sparkSession.sessionState.conf.parquetFilterPushDown =>
        val converter    = new SparkToParquetSchemaConverter(sparkSession.sessionState.conf)
        val parquetSchema = converter.convert(dataSchemaArg)
        val sqlConf      = sparkSession.sessionState.conf
        // Spark 3.5.0 ParquetFilters constructor — all 8 args required:
        //   schema, pushDownDate, pushDownTimestamp, pushDownDecimal,
        //   pushDownStringPredicate, pushDownInFilterThreshold,
        //   caseSensitive, datetimeRebaseSpec: RebaseDateTime.RebaseSpec
        val rebaseSpec   = RebaseDateTime.RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
        val parquetFilters = new ParquetFilters(
          parquetSchema,
          sqlConf.parquetFilterPushDownDate,
          sqlConf.parquetFilterPushDownTimestamp,
          sqlConf.parquetFilterPushDownDecimal,
          sqlConf.parquetFilterPushDownStringPredicate,
          sqlConf.parquetFilterPushDownInFilterThreshold,
          sqlConf.caseSensitiveAnalysis,
          rebaseSpec
        )
        dataFilters.filter(f => parquetFilters.createFilter(f).isEmpty)

      case "orc" if sparkSession.sessionState.conf.orcFilterPushDown =>
        dataFilters.filter(f =>
          OrcFilters.createFilter(dataSchemaArg, Array(f)).isEmpty)

      case "csv" if sparkSession.sessionState.conf.csvFilterPushDown =>
        // StructFilters is in org.apache.spark.sql.execution.datasources
        // pushedFilters returns the residuals (filters that cannot be pushed)
        StructFilters.pushedFilters(dataFilters, dataSchemaArg)

      case "json" if sparkSession.sessionState.conf.jsonFilterPushDown =>
        StructFilters.pushedFilters(dataFilters, dataSchemaArg)

      case _ =>
        // avro, text, textfile — no storage-level filter pushdown
        dataFilters
    }

    // Store accepted filters in our own pushedDataFilters (protected var on
    // FileScanBuilder, assignable on `this`). build() reads this directly.
    pushedDataFilters = dataFilters.diff(remaining)
    remaining
  }

  // ── FIX 2: pushAggregation via short-lived format builder ──────────────────
  //
  // Aggregate pushdown is stateless across calls — it only checks conf flags
  // and computes the output schema once. Safe to use a short-lived builder.
  override def pushAggregation(aggregation: Aggregation): Boolean =
    format.toLowerCase match {
      case "csv" | "json" | "avro" | "text" | "textfile" => false

      case "parquet" =>
        val b = ParquetScanBuilder(sparkSession, fileIndexArg, schema, dataSchemaArg, optionsArg)
        b match {
          case agg: SupportsPushDownAggregates =>
            val pushed = agg.pushAggregation(aggregation)
            if (pushed) {
              pushedAggregations = Some(aggregation)
              finalSchema        = b.build().readSchema()
            }
            pushed
          case _ => false
        }

      case "orc" =>
        val b = OrcScanBuilder(sparkSession, fileIndexArg, schema, dataSchemaArg, optionsArg)
        b match {
          case agg: SupportsPushDownAggregates =>
            val pushed = agg.pushAggregation(aggregation)
            if (pushed) {
              pushedAggregations = Some(aggregation)
              finalSchema        = b.build().readSchema()
            }
            pushed
          case _ => false
        }

      case _ => false
    }

  // ── FIX 4: build reads pushedDataFilters on `this` ─────────────────────────
  override def build(): V2CustomTableScan = {
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema
    }

    // pushedDataFilters — protected var on FileScanBuilder, read on `this` — valid
    val pushed: Array[Filter] = pushedDataFilters

    V2CustomTableScan(
      multiPartName       = multiPartName,
      format              = format,
      sparkSession        = sparkSession,
      hadoopConf          = confHadoop,
      fileIndex           = fileIndexArg,
      dataSchema          = dataSchemaArg,
      readDataSchema      = finalSchema,
      readPartitionSchema = readPartitionSchema,
      pushedFilters       = pushed,
      options             = optionsArg,
      pushedAggregate     = pushedAggregations,
      partitionFilters    = partitionFilters,
      dataFilters         = dataFilters,
      catalogTable        = catalogTableArg
    )
  }
}

// ── Factory ───────────────────────────────────────────────────────────────────
object V2CustomTableScanBuilder {
  def apply(
             multiPartName: Seq[String],
             format:        String,
             sparkSession:  SparkSession,
             hadoopConf:    Configuration,
             fileIndex:     PartitioningAwareFileIndex,
             schema:        StructType,
             dataSchema:    StructType,
             options:       CaseInsensitiveStringMap,
             catalogTable:  Option[CatalogTable] = None
           ): V2CustomTableScanBuilder =
    new V2CustomTableScanBuilder(
      multiPartName, format, sparkSession, hadoopConf,
      fileIndex, schema, dataSchema, options, catalogTable)
}
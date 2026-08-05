package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Statistics, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScan

import java.util.{Objects, OptionalLong}
import scala.collection.JavaConverters.mapAsScalaMapConverter

// ─────────────────────────────────────────────────────────────────────────────
// CHANGE LIST
// ─────────────────────────────────────────────────────────────────────────────
//
// FIX 4 — equals: was comparing to ParquetScan (always false for own type),
//          now compares V2CustomTableScan to V2CustomTableScan.
//   BEFORE: `case p: ParquetScan => ...` meant two identical V2CustomTableScan
//           nodes reading the same table were never equal. This broke:
//             (a) ReuseExchange — Spark's physical optimizer rule that deduplicates
//                 identical ShuffleExchange / BroadcastExchange nodes. It uses
//                 equals() on the physical plan tree; since BatchScanExec delegates
//                 to scan.equals(), every exchange was treated as distinct, causing
//                 redundant shuffles and broadcasts on every multi-join TPC-DS query.
//             (b) Dynamic Partition Pruning (DPP) — the broadcast built for a
//                 dimension table filter could not be reused for the fact table join
//                 because the scans compared unequal.
//             (c) Exchange reuse across CTE / WITH clause references.
//   AFTER:  correct structural equality on all relevant fields.
//
// FIX 5 — hashCode: was getClass.hashCode() — same value for every instance.
//   BEFORE: Spark's ReuseExchange uses a HashMap keyed by exchange plan. A constant
//           hash means all entries land in the same bucket, turning every HashMap
//           lookup into a linear scan. On a 20-join TPC-DS query with 40+ exchanges
//           this is measurably slow even when no reuse is found.
//   AFTER:  hash over the structurally significant fields, same contract as equals.
//
// FIX 6 — estimateStatistics: was delegating to FileScan.super which only returns
//          sizeInBytes from the fileIndex with no row count.
//   BEFORE: CBO (spark.sql.cbo.enabled=true in App.scala) could not use row counts
//           for join ordering because estimateStatistics returned OptionalLong.empty()
//           for numRows. The optimizer fell back to size-based heuristics and made
//           suboptimal broadcast/sort-merge join decisions on TPC-DS star joins.
//   AFTER:  if the catalog table has stats from ANALYZE TABLE, expose both
//           sizeInBytes and numRows to the optimizer. Falls back to FileScan's
//           default (fileIndex.sizeInBytes) when no catalog stats exist.
//           catalogTable is threaded through as a new constructor field.
//
// FIX 7 — removed SupportsRuntimeFiltering override gap
//   FileScan already implements SupportsRuntimeFiltering and filterAttributes()
//   based on partition columns from fileIndex. V2CustomTableScan inherits this
//   correctly — no override needed. The gap was that DPP was commented out in
//   App.scala (line 1064). Document here that enabling DPP in App.scala is the
//   other half of making runtime filtering work end-to-end.
//   See App.scala: spark.sql.optimizer.dynamicPartitionPruning.enabled = true
//                  spark.sql.optimizer.dynamicPartitionPruning.useStats   = true
//                  spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly = true
//
// FIX 8 — stale imports removed (ParquetInputFormat, ParquetOptions,
//          ParquetReadSupport, ParquetWriteSupport, SQLConf,
//          SerializableConfiguration, PartitioningUtils — none used)
// ─────────────────────────────────────────────────────────────────────────────

case class V2CustomTableScan(
                              multiPartName: Seq[String],
                              format: String,
                              sparkSession: SparkSession,
                              hadoopConf: Configuration,
                              fileIndex: PartitioningAwareFileIndex,
                              dataSchema: StructType,
                              readDataSchema: StructType,
                              readPartitionSchema: StructType,
                              pushedFilters: Array[Filter],
                              options: CaseInsensitiveStringMap,
                              pushedAggregate: Option[Aggregation] = None,
                              partitionFilters: Seq[Expression] = Seq.empty,
                              dataFilters: Seq[Expression] = Seq.empty,
                              // FIX 6 — catalogTable needed to expose ANALYZE TABLE stats to CBO
                              catalogTable: Option[CatalogTable] = None)
  extends FileScan {

  override def isSplitable(path: Path): Boolean =
  // aggregate pushdown reads only the footer once — splitting would duplicate that
    pushedAggregate.isEmpty

  override def readSchema(): StructType =
    if (pushedAggregate.nonEmpty) readDataSchema else super.readSchema()

  override def createReaderFactory(): PartitionReaderFactory =
    format.toLowerCase match {
      case "parquet" =>
        ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema,
          readPartitionSchema, pushedFilters, options, pushedAggregate,
          partitionFilters, dataFilters).createReaderFactory()

      case "orc" =>
        OrcScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema,
          readPartitionSchema, options, pushedAggregate, pushedFilters,
          partitionFilters, dataFilters).createReaderFactory()

      case "csv" =>
        CSVScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
          options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "json" =>
        JsonScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
          options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "avro" =>
        AvroScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
          options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "text" =>
        TextScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
          options, partitionFilters, dataFilters).createReaderFactory()

      case "textfile" =>
        CSVScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
          options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()
    }

  // FIX 4 — correct structural equality so ReuseExchange and DPP work
  override def equals(obj: Any): Boolean = obj match {
    case other: V2CustomTableScan =>
      val aggEqual = (pushedAggregate, other.pushedAggregate) match {
        case (Some(a), Some(b)) => AggregatePushDownUtils.equivalentAggregations(a, b)
        case (None, None)       => true
        case _                  => false
      }
      // super.equals (FileScan) checks fileIndex, readDataSchema, readPartitionSchema,
      // partitionFilters, dataFilters — same contract as ParquetScan.equals
      super.equals(other) &&
        format         == other.format         &&
        dataSchema     == other.dataSchema     &&
        options        == other.options        &&
        equivalentFilters(pushedFilters, other.pushedFilters) &&
        aggEqual
    case _ => false
  }

  // FIX 5 — meaningful hash so ReuseExchange HashMap lookups stay O(1)
  override def hashCode(): Int =
    Objects.hash(
      format,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      options,
      fileIndex
    )

  // FIX 6 — expose catalog stats (from ANALYZE TABLE) to CBO for join ordering
  override def estimateStatistics(): Statistics = new Statistics {
    private val catalogStats = catalogTable.flatMap(_.stats)

    override def sizeInBytes(): OptionalLong =
      catalogStats.flatMap(s => Option(s.sizeInBytes))
        .map(b => OptionalLong.of(b.toLong))
        .getOrElse(V2CustomTableScan.super.estimateStatistics().sizeInBytes())

    override def numRows(): OptionalLong =
      catalogStats.flatMap(_.rowCount)
        .map(b => OptionalLong.of(b.toLong))
        .getOrElse(OptionalLong.empty())
  }

  lazy private val (pushedAggregationsStr, pushedGroupByStr) =
    if (pushedAggregate.nonEmpty)
      (seqToString(pushedAggregate.get.aggregateExpressions),
        seqToString(pushedAggregate.get.groupByExpressions))
    else
      ("[]", "[]")

  override def getMetaData(): Map[String, String] =
    super.getMetaData() ++
      Map("PushedFilters"   -> seqToString(pushedFilters)) ++
      Map("PushedAggregation" -> pushedAggregationsStr)    ++
      Map("PushedGroupBy"   -> pushedGroupByStr)
}
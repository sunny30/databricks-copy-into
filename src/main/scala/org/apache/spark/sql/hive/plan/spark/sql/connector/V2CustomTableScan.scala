package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Statistics}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetPartitionReaderFactory, ParquetScan}
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class V2CustomTableScan(
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
                              dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) readDataSchema else super.readSchema()
  }



  override def createReaderFactory(): PartitionReaderFactory = {
    format.toLowerCase match {
      case "parquet" => ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema,
        readPartitionSchema, pushedFilters, options, pushedAggregate,
        partitionFilters, dataFilters).createReaderFactory()

      case "orc" => OrcScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema,
        readPartitionSchema, options, pushedAggregate,pushedFilters,
        partitionFilters, dataFilters).createReaderFactory()

      case "csv" => CSVScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
        options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "json" => JsonScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
        options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "avro" => AvroScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
        options, pushedFilters, partitionFilters, dataFilters).createReaderFactory()

      case "text" => TextScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema,
         options,  partitionFilters, dataFilters).createReaderFactory()
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: ParquetScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual
    case _ => false
  }

  override def estimateStatistics(): Statistics = super.estimateStatistics()

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByExpressions))
  } else {
    ("[]", "[]")
  }

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr)
  }
}


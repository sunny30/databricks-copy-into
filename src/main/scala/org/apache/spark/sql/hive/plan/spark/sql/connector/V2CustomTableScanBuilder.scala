package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScanBuilder
import org.apache.spark.sql.execution.datasources.v2.json.JsonScanBuilder
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScanBuilder
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetScan, ParquetScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.text.TextScanBuilder
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScanBuilder

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class V2CustomTableScanBuilder(
                                     multiPartName: Seq[String],
                                     format: String,
                                     sparkSession: SparkSession,
                                     fileIndex: PartitioningAwareFileIndex,
                                     schema: StructType,
                                     dataSchema: StructType,
                                     options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
   format.toLowerCase match {
     case "parquet" => ParquetScanBuilder(sparkSession, fileIndex, schema,dataSchema, options).pushDataFilters(dataFilters)
     case "orc" => OrcScanBuilder(sparkSession, fileIndex, schema,dataSchema, options).pushDataFilters(dataFilters)
     case "csv" => CSVScanBuilder(sparkSession, fileIndex, schema,dataSchema, options).pushDataFilters(dataFilters)
     case "json" => JsonScanBuilder(sparkSession, fileIndex, schema,dataSchema, options).pushDataFilters(dataFilters)
     case "avro" => AvroScanBuilder(sparkSession, fileIndex, schema,dataSchema, options).pushDataFilters(dataFilters)
     case "text" => Array.empty[Filter]
   }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  override def build(): V2CustomTableScan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }
    V2CustomTableScan(multiPartName,format, sparkSession, hadoopConf, fileIndex, dataSchema, finalSchema,
      readPartitionSchema(), pushedDataFilters, options, pushedAggregations,
      partitionFilters, dataFilters)
  }
}

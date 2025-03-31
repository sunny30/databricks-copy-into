package org.apache.spark.sql.arrow

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class ArrowScan(
                      sparkSession: SparkSession,
                      fileIndex: PartitioningAwareFileIndex,
                      readDataSchema: StructType,
                      readPartitionSchema: StructType,
                      pushedFilters: Array[Filter],
                      options: CaseInsensitiveStringMap,
                      partitionFilters: Seq[Expression] = Seq.empty,
                      dataFilters: Seq[Expression] = Seq.empty)
  extends FileScan {

  override def isSplitable(path: Path): Boolean = {
    ArrowUtils.isOriginalFormatSplitable(
      new ArrowOptions(new CaseInsensitiveStringMap(options).asScala.toMap))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap().asScala.toMap
    val hconf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hconf))
    ArrowPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      new ArrowOptions(options.asScala.toMap),
      Seq.empty[Filter])
  }

  override def dataSchema: StructType = readDataSchema
}

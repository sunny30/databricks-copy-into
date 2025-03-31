package org.apache.spark.sql.arrow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ArrowScanBuilder(
                             sparkSession: SparkSession,
                             fileIndex: PartitioningAwareFileIndex,
                             schema: StructType,
                             dataSchema: StructType,
                             options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  private var filters: Array[Filter] = Array.empty
  private lazy val pushedArrowFilters: Array[Filter] = {
    filters // todo filter validation & pushdown
  }

//  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//    Array.empty[Filter]
//  }


  override def build(): Scan = {
    ArrowScan(
      sparkSession,
      fileIndex,
      readDataSchema(),
      readPartitionSchema(),
      Array.empty[Filter],
      options)
  }
}

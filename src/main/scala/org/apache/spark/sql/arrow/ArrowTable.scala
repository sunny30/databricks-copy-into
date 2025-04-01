package org.apache.spark.sql.arrow

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ArrowTable(
                       name: String,
                       sparkSession: SparkSession,
                       options: CaseInsensitiveStringMap,
                       paths: Seq[String],
                       userSpecifiedSchema: Option[StructType],
                       fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    ArrowUtils.readSchema(files, options)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    ArrowScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    throw new UnsupportedOperationException // fixme implement later
  }

  override def formatName: String = "ARROW"
}

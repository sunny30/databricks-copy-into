package org.apache.spark.sql.arrow

import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ArrowDataSourceV2 extends FileDataSourceV2 {

  private val format = classOf[ArrowFileFormat]


  override def fallbackFileFormat: Class[_ <: FileFormat] = {
    format
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options,paths)
    ArrowTable(tableName, sparkSession, options, paths, None, fallbackFileFormat)
  }

  override def shortName(): String = "arrow"
}

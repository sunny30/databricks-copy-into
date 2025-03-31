package org.apache.spark.sql.arrow

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class ArrowOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends FileSourceOptions(parameters) with Logging {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val originalFormat = parameters
    .get(ArrowOptions.KEY_ORIGINAL_FORMAT)
    .getOrElse(ArrowOptions.DEFAULT_ORIGINAL_FORMAT)
  @deprecated
  val filesystem = parameters
    .get(ArrowOptions.KEY_FILESYSTEM)
    .getOrElse(ArrowOptions.DEFAULT_FILESYSTEM)
}

object ArrowOptions {
  val KEY_ORIGINAL_FORMAT = "originalFormat"
  val DEFAULT_ORIGINAL_FORMAT = "parquet"
  @deprecated
  val KEY_FILESYSTEM = "filesystem"
  val DEFAULT_FILESYSTEM = "hdfs"
}

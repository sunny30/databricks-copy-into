package io.delta.tables.hc

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.catalog.DeltaTableV2

object DeltaTable {

  def getDeltaTable(spark: SparkSession, tbl: String, path: String): DeltaTable = {
    new DeltaTable(
      spark.table(tbl),
      DeltaTableV2(spark, new Path(path)))
  }

}

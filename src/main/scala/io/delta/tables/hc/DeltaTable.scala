package io.delta.tables.hc

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.catalog.DeltaTableV2

object DeltaTable {

  def getDeltaTable(spark: SparkSession, tbl: String, path: String): DeltaTable = {
    new DeltaTable(
      spark.table(tbl),
      DeltaTableV2(spark, new Path(path)))
  }

  def create(spark: SparkSession, tbl: String, path: String):DeltaTable={
    io.delta.tables.DeltaTable.create(spark).tableName(tbl).addColumn("id","INT")
      .addColumn("name", "String")
      .addColumn("age", "INT")
      .partitionedBy("age")
      .property("description", "Sample Delta Table")
      .execute()
  }

}

object CLSPlanMask {
  val CheckedKey = "cls.checked"   // this relation went through the CLS check
  val SecureKey  = "cls.secure"    // this attribute is permitted for the user

  def wasChecked(out: Seq[Attribute]): Boolean = out.exists(_.metadata.contains(CheckedKey))

  def visible[A <: Attribute](out: Seq[A]): Seq[A] =
    if (wasChecked(out)) out.filter(_.metadata.contains(SecureKey)) else out
}

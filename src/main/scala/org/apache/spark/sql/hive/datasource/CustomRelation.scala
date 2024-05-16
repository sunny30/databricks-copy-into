package org.apache.spark.sql.hive.datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class CustomRelation(sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation
  with PrunedFilteredScan with InsertableRelation with TableScan {



  private var _schema: StructType = null

  def setSchema(sch: StructType) = this._schema = sch

  override def schema: StructType = this._schema

  override def buildScan(): RDD[Row] = {
    val spark = SparkSession.active
    import spark.implicits._
    val results = Seq(1).toDF("id")
    results.rdd

  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = buildScan()

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???


}


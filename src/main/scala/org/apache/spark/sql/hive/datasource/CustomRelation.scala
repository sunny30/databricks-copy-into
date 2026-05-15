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
    println("this is build schema")
    val spark = SparkSession.active
    import spark.implicits._
    val results = Seq(("2", "hello", "3"), ("3", "hello", "3"), ("4", "hello", "2"), ("5", "hello", "2")).toDF("col1", "col2", "col3")
    results.rdd

  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println("this is build schema with parameters")
    println(s"schema json ${schema.sql}")
    val spark = SparkSession.active
    import spark.implicits._
    val results = Seq(("2", "hello", "3"), ("3", "hello", "3"), ("4", "hello", "2"), ("5", "hello", "2")).toDF("col1", "col2", "col3")
//    val p = results.select(requiredColumns.head, requiredColumns.tail:_*)
    results.rdd
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    println(s"${data.collect().toString} and flag overwrite is ${overwrite.toString}")
    println("Inside insert")
  }


}


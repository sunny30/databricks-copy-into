package org.apache.spark.sql.hive.plan.may26hack.query

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.StructType

case class ExternalCatalogQueryRelation(sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation
  with PrunedFilteredScan with InsertableRelation with TableScan {



  private var _schema: StructType = null

  def setSchema(sch: StructType) = this._schema = sch

  override def schema: StructType = this._schema

  override def buildScan(): RDD[Row] = {
    val spark = SparkSession.active
    import spark.implicits._
    val results = Seq((2, "hello",3)).toDF("col1", "col2", "col3")
    results.rdd

  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {


    val spark = SparkSession.active
    import spark.implicits._
    val results = if(parameters.contains("pushdown.sql")){
      var sql = parameters.get("pushdown.sql").get.toString
      sql = sql.replaceAll("toprettystring", "")
      println("ExternalCatalogQueryRelation sql is "+sql)
      println(s"External Query Relation schema: ${schema.sql}")
      val parsedPlan = spark.sessionState.sqlParser.parsePlan(sql)
      val analyzedPlan = spark.sessionState.analyzer.execute(parsedPlan)

      analyzedPlan.foreach(p => p.setTagValue(TreeNodeTag[String]("cut-included"), "true"))
      val optimizedPlan = spark.sessionState.optimizer.execute(analyzedPlan)
      optimizedPlan.foreach(p => p.setTagValue(TreeNodeTag[String]("cut-included"), "true"))
      println(s"optimized plan schema ${optimizedPlan.schema.sql}" )
        spark.internalCreateDataFrame(
        spark.sessionState.executePlan(optimizedPlan, CommandExecutionMode.SKIP).toRdd,
        optimizedPlan.schema
      )
     //Seq((1, "success",1)).toDF("col1", "col2", "col3").toDF()

    }else {
     Seq((2, "hello",3)).toDF("col1", "col2", "col3").toDF()
    }
    //    val p = results.select(requiredColumns.head, requiredColumns.tail:_*)
    results.rdd
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    println(s"${data.collect().toString} and flag overwrite is ${overwrite.toString}")
    println("Inside insert")
  }


}
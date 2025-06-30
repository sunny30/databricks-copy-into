package org.apache.spark.sql.hive.plan.spark.sql.parser

import org.apache.calcite.plan.RelOptUtil.Logic
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hudi.parser.HoodieSpark2ExtendedSqlParser
import org.apache.spark.sql.parser.HoodieCommonSqlParser

class HudiIcebergParser(val delegate: ParserInterface) {


  def parsePlan(sqlText:String):LogicalPlan = {

    val icebergParser = new IcebergSparkSqlExtensionsParser(delegate)
    val conf = SparkSession.active.sqlContext.conf
    val hudiParser = new HoodieCommonSqlParser(SparkSession.active, delegate)

    try{
      icebergParser.parsePlan(sqlText)
    }catch {
      case e:Exception => hudiParser.parsePlan(sqlText)
    }

  }

}

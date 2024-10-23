package org.apache.spark.sql.hive.experiment.test

import org.apache.spark.sql.hive.experiment.sql.SQLParser

object SQLApp {

  def main(args: Array[String]): Unit = {
    val text = "select db.t.a,db.t.b from db.t where db.t.a<10"
    val parser =new  SQLParser
    val details = parser.getParsePlanDetails(text)
    val relationDetails = parser.getRelation(sqlText = text)

  }
}

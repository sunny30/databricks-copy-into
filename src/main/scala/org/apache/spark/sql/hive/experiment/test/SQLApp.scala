package org.apache.spark.sql.hive.experiment.test

import org.apache.spark.sql.hive.experiment.sql.SQLParser

object SQLApp {

  def main(args: Array[String]): Unit = {
    val text = "select db.t.a,db.t.b from db.t where db.t.a<10"

    val text1 = "select * from db.t where db.t.a<10"
    val parser =new  SQLParser
    val details = parser.getParsePlanDetails(text)
    val details1 = parser.getParsePlanDetails(text1)
    val relationDetails = parser.getRelation(sqlText = text)
    val metadataDetails = parser.getMetaDataFromPlanDetails(details)
    val metadataDetails1 = parser.getMetaDataFromPlanDetails(details1)
    metadataDetails.length
    metadataDetails1.length

  }
}

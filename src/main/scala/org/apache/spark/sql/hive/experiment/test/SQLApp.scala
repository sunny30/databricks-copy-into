package org.apache.spark.sql.hive.experiment.test

import org.apache.spark.sql.hive.experiment.sql.SQLParser

object SQLApp {

  def main(args: Array[String]): Unit = {
    val text = "select a,b from db.t"
    val parser =new  SQLParser
    parser.getRelation(text)
  }
}

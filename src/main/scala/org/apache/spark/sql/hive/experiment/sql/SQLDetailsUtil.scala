package org.apache.spark.sql.hive.experiment.sql

object SQLDetailsUtil {

  case class QualifiedRelation(dbName: String, tableName: String)

  case class QualifiedColumn(dbName: String, tableName: String, columnName: String)
  case class PlanDetails(optype: String, attributes: Seq[String], expressions: Seq[QualifiedColumn],lineageInfo: Option[Map[String, String]]=None)
}

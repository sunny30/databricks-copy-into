package org.apache.spark.sql.hive.experiment.sql

object SQLDetailsUtil {

  abstract class PlanDetails()

  case class RelationDetails(dbName: String, tableName: String) extends PlanDetails

  case class QualifiedColumn(dbName: String, tableName: String, columnName: String)
  case class InterimPlanDetails(optype: String, attributes: Seq[QualifiedColumn], expressions: Seq[String], lineageInfo: Option[Map[String, String]]=None) extends PlanDetails
}

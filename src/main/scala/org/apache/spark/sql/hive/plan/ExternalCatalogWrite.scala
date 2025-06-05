package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand

case class ExternalCatalogWrite(spark:SparkSession) extends Rule[LogicalPlan]{


  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case insertIntoDataSourceCommand: InsertIntoDataSourceCommand => ExternalCatalogInsertPlan(insertIntoDataSourceCommand)

      case p:LogicalPlan => p
    }
  }

}

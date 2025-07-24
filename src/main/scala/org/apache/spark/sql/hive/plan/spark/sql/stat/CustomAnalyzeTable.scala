package org.apache.spark.sql.hive.plan.spark.sql.stat

import org.apache.spark.sql.catalyst.plans.logical.{AnalyzeTable, LogicalPlan, UnaryCommand}



case class CustomAnalyzeTable(
                         child: LogicalPlan,
                         partitionSpec: Map[String, Option[String]],
                         noScan: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): CustomAnalyzeTable =
    copy(child = newChild)
}


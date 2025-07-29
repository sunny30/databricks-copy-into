package org.apache.spark.sql.hive.plan.spark.sql.stat

import org.apache.spark.sql.catalyst.plans.logical.{AnalyzeTable, LogicalPlan, UnaryCommand}



case class CustomAnalyzeTable(
                         child: LogicalPlan,
                         partitionSpec: Map[String, Option[String]],
                         noScan: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): CustomAnalyzeTable =
    copy(child = newChild)
}


case class CustomAnalyzeColumn(
                          child: LogicalPlan,
                          columnNames: Option[Seq[String]],
                          allColumns: Boolean) extends UnaryCommand {
  require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
    "mutually exclusive. Only one of them should be specified.")

  override protected def withNewChildInternal(newChild: LogicalPlan): CustomAnalyzeColumn =
    copy(child = newChild)
}


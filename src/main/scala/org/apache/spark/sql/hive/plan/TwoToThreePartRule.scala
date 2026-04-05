package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, CreateTableCommand}

class TwoToThreePartRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {


  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp{
    case c@CreateTableCommand(table, ignoreIfExists) => c
    case cd@CreateDataSourceTableCommand(table,ignoreIfExists ) => cd
  }

}

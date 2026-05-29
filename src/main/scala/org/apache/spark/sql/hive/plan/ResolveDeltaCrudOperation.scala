package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, MergeIntoTable, SubqueryAlias, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ShowCatalogViews

class ResolveDeltaCrudOperation(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {


    case d: DeleteFromTable =>
      val newQuery = CLSUtils.removeSecureProjection(d.table)
      d.copy(table = newQuery)

    case mergeIntoTable: MergeIntoTable =>
      val newSource = CLSUtils.removeSecureProjection(mergeIntoTable.sourceTable)
      val newTarget = CLSUtils.removeSecureProjection(mergeIntoTable.targetTable)
      mergeIntoTable.copy(sourceTable = newSource, targetTable = newTarget)

    case u:UpdateTable  =>
      val newQuery = CLSUtils.removeSecureProjection(u.table)
      u.copy(table = newQuery)


//    case f@FakeLogicalPlan(exprs: Seq[Expression],
//    children: Seq[LogicalPlan]) =>
//      val newChildren = children.map(f => f match {
//        case s: SubqueryAlias => CLSUtils.removeSecureProjection(s)
//        case pl:LogicalPlan => pl
//      })
//      f.copy(children = newChildren)


    case pl: LogicalPlan => pl
  }



}

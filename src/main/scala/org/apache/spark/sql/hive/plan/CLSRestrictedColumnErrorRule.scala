package org.apache.spark.sql.hive.plan
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{TempResolvedColumn, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeInto, LogicalPlan, MergeIntoTable}
import org.apache.spark.sql.catalyst.rules.Rule

class CLSRestrictedColumnErrorRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp { node =>
      if (node.childrenResolved &&
        !isMergeNodeAwaitingReferenceResolution(node) &&
        //node.children.exists(_.find(CLSUtils.isSecureProjection).isDefined) &&
        (node.expressions.exists(containsRestrictedColumnResolutionFailure) ||
          node.missingInput.nonEmpty)) {
        throw new AnalysisException(
          "One or more referenced columns are either not found in the accessible schema or the current user does not have access to them.")
      }
    }
    plan
  }

  private def isMergeNodeAwaitingReferenceResolution(node: LogicalPlan): Boolean = {
    node match {
      case merge: MergeIntoTable => !merge.resolved
      case merge: DeltaMergeInto => !merge.resolved
      case _ if node.getClass.getName == "org.apache.spark.sql.delta.util.AnalysisHelper$FakeLogicalPlan" =>
        CLSUtils.isDeltaMergeAnalysisStack
      case _ => false
    }
  }

  private def containsRestrictedColumnResolutionFailure(expression: Expression): Boolean = {
    expression.find {
      case _: UnresolvedAttribute => true
      case _ => false
    }.isDefined
  }
}
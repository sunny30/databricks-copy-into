package org.apache.spark.sql.hive.plan.spark.sql.execution

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, GlobalTempView, LocalTempView, ViewType}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisOnlyCommand, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateViewCommand, RunnableCommand}

case class NonDefaultCatalogCreateViewCommand(
                              name: TableIdentifier,
                              userSpecifiedColumns: Seq[(String, Option[String])],
                              comment: Option[String],
                              properties: Map[String, String],
                              originalText: Option[String],
                              plan: LogicalPlan,
                              allowExisting: Boolean,
                              replace: Boolean,
                              viewType: ViewType,
                              isAnalyzed: Boolean = false,
                              referredTempFunctions: Seq[String] = Seq.empty)
  extends RunnableCommand with AnalysisOnlyCommand {


  override protected def withNewChildrenInternal(
                                                  newChildren: IndexedSeq[LogicalPlan]): NonDefaultCatalogCreateViewCommand = {
    assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  // `plan` needs to be analyzed, but shouldn't be optimized so that caching works correctly.
  override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(
      isAnalyzed = true,
      // Collect the referred temporary functions from AnalysisContext
      referredTempFunctions = analysisContext.referredTempFunctionNames.toSeq)
  }

  private def isTemporary = viewType == LocalTempView || viewType == GlobalTempView

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }
}

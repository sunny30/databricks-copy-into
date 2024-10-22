package org.apache.spark.sql.hive.experiment.sql

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Except, Expand, Filter, Generate, GlobalLimit, Intersect, Join, LocalLimit, LogicalPlan, LogicalPlanVisitor, Offset, Pivot, Project, RebalancePartitions, Repartition, RepartitionByExpression, Sample, ScriptTransformation, Sort, Tail, Union, Window, WithCTE}
import org.apache.spark.sql.hive.experiment.sql.SQLDetailsUtil.{PlanDetails, QualifiedColumn}

object ParsedPlanMetadataVisitor extends LogicalPlanVisitor[PlanDetails]{

  override def default(p: LogicalPlan):PlanDetails = PlanDetails("NA", Seq.empty[String], Seq.empty[QualifiedColumn])

  override def visitAggregate(p: Aggregate): PlanDetails = ???

  override def visitDistinct(p: Distinct): PlanDetails = ???

  override def visitExcept(p: Except): PlanDetails = ???

  override def visitExpand(p: Expand): PlanDetails = ???

  override def visitFilter(p: Filter): PlanDetails = ???

  override def visitGenerate(p: Generate): PlanDetails = ???

  override def visitGlobalLimit(p: GlobalLimit): PlanDetails = ???

  override def visitOffset(p: Offset): PlanDetails = ???

  override def visitIntersect(p: Intersect): PlanDetails = ???

  override def visitJoin(p: Join): PlanDetails = ???

  override def visitLocalLimit(p: LocalLimit): PlanDetails = ???

  override def visitPivot(p: Pivot): PlanDetails = ???

  override def visitProject(p: Project): PlanDetails = ???

  override def visitRepartition(p: Repartition): PlanDetails = ???

  override def visitRepartitionByExpr(p: RepartitionByExpression): PlanDetails = ???

  override def visitRebalancePartitions(p: RebalancePartitions): PlanDetails = ???

  override def visitSample(p: Sample): PlanDetails = ???

  override def visitScriptTransform(p: ScriptTransformation): PlanDetails = ???

  override def visitUnion(p: Union): PlanDetails = ???

  override def visitWindow(p: Window): PlanDetails = ???

  override def visitTail(p: Tail): PlanDetails = ???

  override def visitSort(sort: Sort): PlanDetails = ???

  override def visitWithCTE(p: WithCTE): PlanDetails = ???
}

package org.apache.spark.sql.hive.experiment.sql

import org.apache.calcite.sql.parser.SqlParser
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Except, Expand, Filter, Generate, GlobalLimit, Intersect, Join, LeafNode, LocalLimit, LogicalPlan, LogicalPlanVisitor, Offset, Pivot, Project, RebalancePartitions, Repartition, RepartitionByExpression, Sample, ScriptTransformation, Sort, Tail, Union, Window, WithCTE}
import org.apache.spark.sql.hive.experiment.sql.SQLDetailsUtil.{InterimPlanDetails, PlanDetails, QualifiedColumn}

object ParsedPlanMetadataVisitor extends LogicalPlanVisitor[PlanDetails] {

  def attributeDetails(at: Attribute): QualifiedColumn = {
    val ats = at.asInstanceOf[UnresolvedAttribute]
    if (ats.nameParts.length == 4) {
      QualifiedColumn(ats.nameParts(0), ats.nameParts(1), ats.nameParts(2), ats.nameParts(3))
    } else if (ats.nameParts.length == 3) {
      QualifiedColumn("hive",ats.nameParts(0), ats.nameParts(1), ats.nameParts(2))
    } else if (ats.nameParts.length == 2) {
      QualifiedColumn("hive", "default", ats.nameParts(0), ats.nameParts(1))
    }else {
      QualifiedColumn("hive","default", "default", ats.nameParts(0))
    }
  }

  override def default(p: LogicalPlan): InterimPlanDetails = InterimPlanDetails("NA", Seq.empty[QualifiedColumn], Seq.empty[String])

  override def visitAggregate(p: Aggregate): InterimPlanDetails = {
    val colDetails = p.aggregateExpressions.flatMap(e =>
      e.references.map(at => attributeDetails(at)
      )
    )

    val sqlExpressions = p.aggregateExpressions.map(e => e.sql)
    InterimPlanDetails(p.toString(), colDetails, sqlExpressions)
  }

  override def visitDistinct(p: Distinct): InterimPlanDetails = {
    val colDetails = p.distinctKeys.flatMap(e =>
      e.flatMap(ex => ex.references.map(at =>
        attributeDetails(at)
      )
      )
    ).toSeq

    val sqlExpressions = p.distinctKeys.flatMap(ex => ex.map(e => e.sql)).toSeq
    InterimPlanDetails(p.toString(), colDetails, sqlExpressions)
  }

  override def visitExcept(p: Except): InterimPlanDetails = ???

  override def visitExpand(p: Expand): InterimPlanDetails = ???

  override def visitFilter(p: Filter): InterimPlanDetails = {
    val colDetails = p.condition.references.map(at => attributeDetails(at)).toSeq
    val sqlExpressions = Seq(p.condition.sql)
    InterimPlanDetails(p.toString(), colDetails, sqlExpressions)

  }

  override def visitGenerate(p: Generate): InterimPlanDetails = ???

  override def visitGlobalLimit(p: GlobalLimit): InterimPlanDetails = ???

  override def visitOffset(p: Offset): InterimPlanDetails = ???

  override def visitIntersect(p: Intersect): InterimPlanDetails = ???

  override def visitJoin(p: Join): InterimPlanDetails = ???

  override def visitLocalLimit(p: LocalLimit): InterimPlanDetails = ???

  override def visitPivot(p: Pivot): InterimPlanDetails = ???

  override def visitProject(p: Project): InterimPlanDetails = {

    val colDetails = p.projectList.flatMap(ne => ne match {
      case u: UnresolvedStar =>
       val dt =  p.collectLeaves().map(pl => pl match {
          case l:LeafNode =>
            (new SQLParser).getRelationDetails(l)
          case pl:LogicalPlan => ParsedPlanMetadataVisitor.visit(pl)
        })
       dt.flatMap( d => d.getRelationalDetails.map(x => QualifiedColumn(x.catalogName,x.dbName, x.tableName, "*")))
      case _ => ne.flatMap(e => e.references.map(at => {
        attributeDetails(at)
      }
    ))
    })

    val sqlExpressions = p.projectList.flatMap(ex => ex.map(e => e.sql)).toSeq
    InterimPlanDetails(p.toString(), colDetails, sqlExpressions)
  }

  override def visitRepartition(p: Repartition): InterimPlanDetails = ???

  override def visitRepartitionByExpr(p: RepartitionByExpression): InterimPlanDetails = ???

  override def visitRebalancePartitions(p: RebalancePartitions): InterimPlanDetails = ???

  override def visitSample(p: Sample): InterimPlanDetails = ???

  override def visitScriptTransform(p: ScriptTransformation): InterimPlanDetails = ???

  override def visitUnion(p: Union): InterimPlanDetails = ???

  override def visitWindow(p: Window): InterimPlanDetails = ???

  override def visitTail(p: Tail): InterimPlanDetails = ???

  override def visitSort(sort: Sort): InterimPlanDetails = ???

  override def visitWithCTE(p: WithCTE): InterimPlanDetails = ???
}

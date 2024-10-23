package org.apache.spark.sql.hive.experiment.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.experiment.sql.SQLDetailsUtil.{InterimPlanDetails, PlanDetails, RelationDetails}

class SQLParser extends SparkSqlParser{

  override def parsePlan(sqlText: String): LogicalPlan = super.parsePlan(sqlText)


  def getRelation(sqlText: String):Seq[RelationDetails]={
    val plan = parsePlan(sqlText)
    plan.collectLeaves()
      .map(p=>{
        val mident = p.asInstanceOf[UnresolvedRelation]
      .multipartIdentifier
        if(mident.length == 2){
          RelationDetails(mident(0), mident(1))
        }else {
          RelationDetails("default", mident(0))
        }
      })
  }

  def getRelationDetails(p:LeafNode):PlanDetails = {

    val mident = p.asInstanceOf[UnresolvedRelation]
      .multipartIdentifier
    if (mident.length == 2) {
      RelationDetails(mident(0), mident(1))
    } else {
      RelationDetails("default", mident(0))
    }
  }




  def getParsePlanDetails(sqlText: String):Seq[PlanDetails]={
    val plan = parsePlan(sqlText)
    plan.map {
      case l: LeafNode => getRelationDetails(l)
      case pl: LogicalPlan => ParsedPlanMetadataVisitor.visit(pl)
    }
  }

}

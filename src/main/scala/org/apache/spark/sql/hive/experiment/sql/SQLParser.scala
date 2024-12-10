package org.apache.spark.sql.hive.experiment.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.experiment.sql.SQLDetailsUtil.{InterimPlanDetails, PlanDetails, QualifiedColumns, RelationDetails}

class SQLParser extends SparkSqlParser{

  override def parsePlan(sqlText: String): LogicalPlan = super.parsePlan(sqlText)


  def getRelation(sqlText: String):Seq[RelationDetails]={
    val plan = parsePlan(sqlText)
    plan.collectLeaves()
      .map(p=>{
        val mident = p.asInstanceOf[UnresolvedRelation]
      .multipartIdentifier
        if(mident.length == 2){
          RelationDetails("hive",mident(0), mident(1))
        } else if (mident.length == 3) {
          RelationDetails(mident(0), mident(1), mident(2))
        }else {
          RelationDetails("hive","default", mident(0))
        }
      })
  }

  def getRelationDetails(p:LeafNode):PlanDetails = {

    val mident = p.asInstanceOf[UnresolvedRelation]
      .multipartIdentifier
    if (mident.length == 2) {
      RelationDetails("hive", mident(0), mident(1))
    }else if(mident.length == 3){
      RelationDetails(mident(0),mident(1), mident(2))
    } else {
      RelationDetails("hive","default", mident(0))
    }
  }




  def getParsePlanDetails(sqlText: String):Seq[PlanDetails]={
    val plan = parsePlan(sqlText)
    plan.map {
      case l: LeafNode => getRelationDetails(l)
      case pl: LogicalPlan => ParsedPlanMetadataVisitor.visit(pl)
    }
  }

  def getMetaDataFromPlanDetails(planDetails: Seq[PlanDetails]):Seq[QualifiedColumns] = {
    SQLDetailsUtil.getQualifiedColumns(planDetails.flatMap(pd => pd.getRelationalDetails))
  }

}

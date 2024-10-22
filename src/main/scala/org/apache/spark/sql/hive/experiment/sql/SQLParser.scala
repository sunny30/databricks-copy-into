package org.apache.spark.sql.hive.experiment.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.experiment.sql.SQLDetailsUtil.{PlanDetails, QualifiedRelation}

class SQLParser extends SparkSqlParser{

  override def parsePlan(sqlText: String): LogicalPlan = super.parsePlan(sqlText)


  def getRelation(sqlText: String):Seq[QualifiedRelation]={
    val plan = parsePlan(sqlText)
    plan.collectLeaves()
      .map(p=>{
        val mident = p.asInstanceOf[UnresolvedRelation]
      .multipartIdentifier
        if(mident.length == 2){
          QualifiedRelation(mident(0), mident(1))
        }else {
          QualifiedRelation("default", mident(0))
        }
      })
  }


//  def getParsePlanDetails(sqlText: String):Seq[PlanDetails]={
//    val plan = parsePlan(sqlText)
//    plan.foreach(p => )
//  }

}

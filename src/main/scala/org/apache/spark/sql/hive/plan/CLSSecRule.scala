package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedLeafNode, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ViewUnresolvedRelation

class CLSSecRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {


  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp{
    //case p: Project if CLSUtils.isSecureTableProjection(p)=> p.child

    case u@UnresolvedRelation(multipartIdentifier: Seq[String], _, _) => ViewUnresolvedRelation(u)

    case plan: LogicalPlan => plan
  }

}

package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedLeafNode, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ViewUnresolvedRelation

class CLSSecRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  def isCustomProjectionView(plan: LogicalPlan): Boolean = {
    plan.getTagValue(TreeNodeTag[String]("custom-view-projection")).isDefined
  }

  def isViewPlanContainsStar(project: Project):Boolean = {
    project.projectList.exists {
      case u: UnresolvedStar => true
      case e: NamedExpression => false
    }
  }


  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp{
    //case p: Project if CLSUtils.isSecureTableProjection(p)=> p.child

    case proj: Project if isCustomProjectionView(proj)   =>
      println("Inside custom view Projection")
      proj.unsetTagValue(TreeNodeTag[String]("custom-view-projection"))
      changeViewProjectionForCustomAttributes(proj)

    case prj:Project if CLSUtils.isViewsPlan(prj) && isViewPlanContainsStar(prj) && prj.child.resolved =>
      descomposeStarInViewTextPlan(prj)

    case u@UnresolvedRelation(multipartIdentifier: Seq[String], _, _) if CLSUtils.isViewsPlan(u)=>
      //CLSUtils.tagViewPlan(u)
      println("View unresolved relation")
      ViewUnresolvedRelation(u)

    case plan: LogicalPlan => plan
  }


  def changeViewProjectionForCustomAttributes(project: Project):Project={
    val customProjLists:Seq[NamedExpression] = project.projectList.zip(project.child.output).map {
      case (a@ Alias(_, name), b) => Alias(b,name)()
      case (e: NamedExpression, op) => e
    }
    print("custom proj lists "+ customProjLists.map(ex=>ex.sql).mkString(","))
    val proj = project.copy(projectList = customProjLists)
   // proj.unsetTagValue(TreeNodeTag[String]("custom-view-projection"))
    proj
  }

  def descomposeStarInViewTextPlan(project: Project):Project={

    val projections = project.projectList.flatMap {
      case unresolvedStar: UnresolvedStar =>
      println("Star found "+project.child.metadataOutput.mkString(","))
        project.child.output
      case namedExpression: NamedExpression => Seq(namedExpression)
    }
    println("Resolved child value "+project.child.resolved)
    println("Star decompose "+projections.map(e=>e.sql).mkString(","))
    project.copy(projectList = projections)
  }

}



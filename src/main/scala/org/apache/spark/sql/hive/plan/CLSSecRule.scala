package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedLeafNode, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnresolvedWith}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ViewUnresolvedRelation
import org.apache.spark.sql.parser.With

class CLSSecRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  def isCustomProjectionView(plan: LogicalPlan): Boolean = {
    plan.getTagValue(TreeNodeTag[String]("custom-view-projection")).isDefined
  }

  def isViewPlanContainsStar(project: Project): Boolean = {
    project.projectList.exists {
      case expression if expression.find {
        case _: Star => true
        case _       => false
      }.isDefined => true
      case _: NamedExpression => false
    }
  }

  // Collect ALL CTE-defined names from the plan including inside subqueries
  // Plain plan.collect misses CTEs defined inside scalar subqueries / EXISTS / IN
  private def collectCTENames(plan: LogicalPlan): Set[String] = {
    val names = scala.collection.mutable.Set[String]()
    plan.transformUpWithSubqueries {
      case w@UnresolvedWith(_, cteDefs) => // ← UnresolvedWith not With
        cteDefs.foreach { case (name, _) => names += name }
        w
      case other => other
    }
    names.toSet
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {

    // Collect CTE names before transformation
    // Covers CTEs at top level AND inside subquery expressions
    val cteNames: Set[String] = collectCTENames(plan)

    plan.transformUpWithSubqueries {

      // Case 1: Custom view projection — fix column aliasing
      // Fires on Project nodes tagged with "custom-view-projection"
      case proj: Project
        if isCustomProjectionView(proj) &&
          proj.child.resolved &&
          !isViewPlanContainsStar(proj) =>
        logInfo("Inside custom view Projection")
        proj.unsetTagValue(TreeNodeTag[String]("custom-view-projection"))
        changeViewProjectionForCustomAttributes(proj)

      // Case 2: Star expansion in view text plan
      // Fires on view-tagged Project nodes that contain *
      case prj: Project
        if CLSUtils.isViewsPlan(prj) &&
          isViewPlanContainsStar(prj) &&
          prj.child.resolved =>
        descomposeStarInViewTextPlan(prj)

      // Case 3: UnresolvedRelation inside view context
      // Wrap as ViewUnresolvedRelation to preserve isViewsPlan tag
      // through tree transformations in CustomDataSourceAnalyzer
      //
      // CTE guard: skip if name matches a CTE alias
      //   - single-part name matching a CTE name → CTE reference, never a real table
      //   - multi-part names cannot be CTE aliases → always check catalog
      //
      // Without this guard: WITH t AS (...) SELECT * FROM t
      //   if real table `t` also exists, relationExists("t") = true
      //   → wraps CTE reference as ViewUnresolvedRelation
      //   → resolver binds physical table instead of CTE → wrong results
      //   → in CLS context: wrong secure projection → security-sensitive misbinding
      case u @ UnresolvedRelation(multipartIdentifier, _, _)
        if CLSUtils.isViewsPlan(u) &&
          !(multipartIdentifier.size == 1 &&
            cteNames.contains(multipartIdentifier.head)) =>
        CLSUtils.tagViewPlan(u)
        if (CLSUtils.relationExists(multipartIdentifier))
          ViewUnresolvedRelation(u)
        else
          u

      case plan: LogicalPlan => plan
    }
  }

  def changeViewProjectionForCustomAttributes(project: Project): Project = {
    val customProjLists: Seq[NamedExpression] =
      project.projectList.zip(project.child.output).map {
        case (Alias(_, name), b) => Alias(b, name)()
        case (e: NamedExpression, _) => e
      }
    print("custom proj lists " + customProjLists.map(_.sql).mkString(","))
    project.copy(projectList = customProjLists)
  }

  def descomposeStarInViewTextPlan(project: Project): Project = {
    val projections = project.projectList.flatMap {
      case _: Star => project.child.output
      case namedExpression: NamedExpression => Seq(namedExpression)
    }
    println("Star decompose " + projections.map(_.sql).mkString(","))
    project.copy(projectList = projections)
  }
}


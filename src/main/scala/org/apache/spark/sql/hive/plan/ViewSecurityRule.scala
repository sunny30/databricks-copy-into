package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, UnaryNode, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.util.AnalysisHelper

class ViewSecurityRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp{

    case c:CustomView =>
      c.member

    case pl: LogicalPlan =>
        pl

    }

}


case class CustomView(
                  desc: CatalogTable,
                  member: LogicalPlan) extends LeafNode {
 // require(!isTempViewStoringAnalyzedPlan || child.resolved)

  override def output: Seq[Attribute] = member.output

  override def metadataOutput: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"Custom View (${desc.identifier}, ${output.mkString("[", ",", "]")})"
  }

//  override def doCanonicalize(): LogicalPlan =  {
//      child
//  }

  def isTempViewStoringAnalyzedPlan: Boolean =
    false

  // When resolving a SQL view, we use an extra Project to add cast and alias to make sure the view
  // output schema doesn't change even if the table referenced by the view is changed after view
  // creation. We should remove this extra Project during canonicalize if it does nothing.
  // See more details in `SessionCatalog.fromCatalogTable`.
  private def canRemoveProject(p: Project): Boolean = {
    false
  }

//  override protected def withNewChildInternal(newChild: LogicalPlan): CustomView =
//    copy(child = newChild)
}

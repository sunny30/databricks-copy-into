package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowViews, UnaryCommand}
import org.apache.spark.sql.types.{BooleanType, StringType}

case class ShowCatalogViews(
                      namespace: LogicalPlan,
                      pattern: Option[String],
                      override val output: Seq[Attribute] = ShowViews.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowCatalogViews =
    copy(namespace = newChild)
}

object ShowCatalogViews {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("viewName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)())
}


case class RenameCatalogView(
                        child: LogicalPlan,
                        newName: Seq[String],
                        isView: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RenameCatalogView =
    copy(child = newChild)
}


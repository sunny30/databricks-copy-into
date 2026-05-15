package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, ShowColumns, UnaryNode, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.TableSchemaChangeCatalog
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{DeltaCommand, ShowDeltaTableColumnsCommand, TableColumns}
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.command.{LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table

class ViewSecurityRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp{

    case c:CustomView =>
      c.member

    case showDeltaTableColumnsCommand: ShowDeltaTableColumnsCommand =>
        SecureShowDeltaColumnCommand(showDeltaTableColumnsCommand)

    case cmd @ ShowColumns(child @ ResolvedTable(_, _, table: V2Table, _), namespace, _)  =>
      SecureShowColumnsCommand(cmd, table)

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


case class SecureShowDeltaColumnCommand(s:ShowDeltaTableColumnsCommand) extends LeafRunnableCommand with DeltaCommand{

  override val output: Seq[Attribute] = s.output

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = s.run(sparkSession)
    val deltaTable = getDeltaTable(s.child, "SHOW COLUMNS")
    try {
      val tid = deltaTable.v1Table.identifier
      val (catalogName, dbName, tableName) = (tid.catalog.getOrElse("default"), tid.database.getOrElse("default"), tid.table)
      val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
      val secureCatalogTable = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(dbName, tableName)
      val secureColumns = secureCatalogTable.schema.map(f => f.name).toSet
      val secureRows = rows.filter(r => (secureColumns).contains(r.get(0).toString))
      secureRows
    }catch {
      case e:Exception => return rows
    }
  }


}

case class SecureShowColumnsCommand(plan: ShowColumns, v2Table: V2Table) extends LeafRunnableCommand{
  override val output: Seq[Attribute] = toAttributes(ExpressionEncoder[TableColumns]().schema)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tid = v2Table.v1Table.identifier
    val (catalogName, dbName, tableName) = (tid.catalog.getOrElse("default"), tid.database.getOrElse("default"), tid.table)
    val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
    val secureCatalogTable = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(dbName, tableName)
    val secureColumns = secureCatalogTable.schema.map(f => f.name)
    secureColumns.map{ x => Row(x) }
  }
}

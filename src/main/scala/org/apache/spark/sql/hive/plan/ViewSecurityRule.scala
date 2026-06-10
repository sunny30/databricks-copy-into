package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedAttribute}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_STORING_ANALYZED_PLAN
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, ExprId}
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

  override def apply(plan: LogicalPlan): LogicalPlan = {

    // Pass 1: enforce CLS — CustomView still intact
    plan.transformUpWithSubqueries {
      case node: LogicalPlan =>
        node.children.foreach {
          case c: CustomView =>
            val declaredNames = c.desc.schema.fields
              .map(_.name.toLowerCase).toSet
            val secureNames = c.secureOutput
              .map(_.name.toLowerCase).toSet
            val restrictedNames = declaredNames -- secureNames

            if (restrictedNames.nonEmpty) {
              node.expressions.foreach { expr =>
                expr.foreach {
                  case u: UnresolvedAttribute =>
                    val colName = u.nameParts.last.toLowerCase
                    if (restrictedNames.contains(colName)) {
                      throw new AnalysisException(
                        s"Access denied: column '${u.nameParts.last}' " +
                          s"in view '${c.desc.identifier}'"
                      )
                    }
                  case _ =>
                }
              }
            }
          case _ =>
        }
        node
    }

    // Pass 2: substitute CustomView → member
    plan.transformUpWithSubqueries {
      case c: CustomView =>
        c.member

      case cmd: ShowDeltaTableColumnsCommand =>
        SecureShowDeltaColumnCommand(cmd)

      case cmd@ShowColumns(
      child@ResolvedTable(_, _, table: V2Table, _), namespace, _) =>
        SecureShowColumnsCommand(cmd, table)

      case pl: LogicalPlan => pl
    }
  }

}


case class CustomView(
                  desc: CatalogTable,
                  member: LogicalPlan,
                  secureOutput: Seq[Attribute]) extends LeafNode {
 // require(!isTempViewStoringAnalyzedPlan || child.resolved)

  override def output: Seq[Attribute] = secureOutput

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


class EnforceCLSAccess(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(">>> EnforceCLSAccess firing")
    println(">>> plan:\n" + plan.treeString)

    plan.transformUpWithSubqueries {
      // Only check nodes that are DIRECT children of CustomView
      // i.e. the node sits immediately above a CustomView in the tree
      case node: LogicalPlan =>
        node.children.foreach {
          case c: CustomView =>
            val secureNames = c.secureOutput
              .map(_.name.toLowerCase).toSet
            val memberNames = c.member.output
              .map(_.name.toLowerCase).toSet
            val restrictedNames = memberNames -- secureNames

            node.expressions.foreach { expr =>
              expr.foreach {
                case attr: AttributeReference
                  if restrictedNames.contains(attr.name.toLowerCase) =>
                  throw new AnalysisException(
                    s"Access denied: column '${attr.name}' " +
                      s"in view '${c.desc.identifier}'"
                  )
                case u: UnresolvedAttribute
                  if restrictedNames.contains(u.nameParts.last.toLowerCase) =>
                  throw new AnalysisException(
                    s"Access denied: column '${u.nameParts.last}' " +
                      s"in view '${c.desc.identifier}'"
                  )
                case _ =>
              }
            }
          case _ =>
        }
        node
    }
  }
}
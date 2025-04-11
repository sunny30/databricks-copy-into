package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.{V2CommandExec, V2SessionCatalog}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table

import scala.collection.mutable.ArrayBuffer

/**
 * Physical plan node for showing views.
 */
case class ShowViewsExec(
                           output: Seq[Attribute],
                           catalog: TableCatalog,
                           namespace: Seq[String],
                           pattern: Option[String]) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    val tables = catalog.listTables(namespace.toArray).map(ti => catalog.loadTable(ti, null)).
      filter(tbl => tbl match {
        case V2Table(v1Table) if v1Table.tableType == CatalogTableType.VIEW => true
        case _ => false
      }).map(tbl => {
        val ti = tbl.asInstanceOf[V2Table].v1Table.identifier
        Identifier.of(ti.database.toArray, ti.table)
      })
    tables.map { table =>
      if (pattern.map(StringUtils.filterPattern(Seq(table.name()), _).nonEmpty).getOrElse(true)) {
        rows += toCatalystRow(table.namespace().quoted, table.name(), isTempView(table))
      }
    }

    rows.toSeq
  }

  private def isTempView(ident: Identifier): Boolean = {
    catalog match {
      case s: V2SessionCatalog => s.isTempView(ident)
      case _ => false
    }
  }
}


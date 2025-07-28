package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{DescribeRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.CatalogDescribeViewCmd
import org.apache.spark.sql.hive.plan.spark.sql.stat.{CustomAnalyzeColumn, CustomAnalyzeColumnCommand, CustomAnalyzeTable, CustomAnalyzeTableCommand}

class DescribeViewRelationRule(session: SparkSession)
  extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {

      case ca@CustomAnalyzeColumn(r@ResolvedTable(tableCatalog: TableCatalog,identifier: Identifier ,table: Table , outputAttributes: Seq[Attribute]), columns, allColumns) =>
        r.table match {
          case v: V2Table =>
            val tableIdent = v.v1Table.identifier
            CustomAnalyzeColumnCommand(tableIdent, columns, allColumns )

          case _ => throw new IllegalStateException("Only V2Table is allowed for Analyze SQL")
        }

      case c@CustomAnalyzeTable(r@ResolvedTable(tableCatalog: TableCatalog,identifier: Identifier ,table: Table , outputAttributes: Seq[Attribute]), _ , _) =>
        r.table match {
          case v: V2Table =>
            val catalogName = v.v1Table.identifier.catalog.getOrElse("spark_catalog")
            val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
            val tableIdent = v.v1Table.identifier
            CustomAnalyzeTableCommand(plugin = plugin, tableIdent = tableIdent)

          case v1: V1Table =>
            val catalogName =v1.v1Table.identifier.catalog.getOrElse("spark_catalog")
            val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
            val tableIdent = v1.v1Table.identifier
            CustomAnalyzeTableCommand(plugin = plugin, tableIdent = tableIdent)

          case _ => throw new IllegalStateException("Only V2Table is allowed for Analyze SQL")

        }

      case d@DescribeRelation(r@ResolvedTable(tableCatalog: TableCatalog,identifier: Identifier ,table: Table , outputAttributes: Seq[Attribute]), partitionSpec: TablePartitionSpec, isExtended: Boolean, _) =>
        r.table match {
          case V2Table(v1Table) => if (v1Table.tableType == CatalogTableType.VIEW) {
            CatalogDescribeViewCmd(v1Table.identifier.catalog.get,
              v1Table.identifier.database.getOrElse("default"),
              v1Table.identifier.table, d.isExtended)
          } else {
            d
          }
          case _ => d
        }
      case pl:LogicalPlan => pl
    }
  }


}

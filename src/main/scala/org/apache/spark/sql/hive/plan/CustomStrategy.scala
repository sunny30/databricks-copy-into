package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{DescribeColumn, DescribeRelation, LogicalPlan, ShowTables, ShowViews}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.{RenameCatalogView, RenameCatalogViewExec, SecureDescribeColumnExec, SecureDescribeTableExec, ShowCatalogViews, ShowViewsExec}

import scala.collection.JavaConverters._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.DescribeTableExec
import org.apache.spark.sql.hive.catalog.UnityCatalog

object CustomStrategy extends Strategy with Serializable  {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    plan match {

      case ShowCatalogViews(ResolvedNamespace(catalog, ns), pattern, output) =>
        ShowViewsExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

      case RenameCatalogView(r @ ResolvedTable(catalog, oldIdent, _, _), newIdent, isView) =>
        RenameCatalogViewExec(
          catalog,
          oldIdent,
          newIdent.asIdentifier,
          ()=>None,
          SparkSession.active.sharedState.cacheManager.cacheQuery) :: Nil

      case d@DescribeRelation(r: ResolvedTable,_,_,_) if !r.catalog.name().equalsIgnoreCase("live")=>
        SecureDescribeTableExec(d)::Nil

      case dc@DescribeColumn(r: ResolvedTable, column: Attribute, isExtended, output) =>
        SecureDescribeColumnExec(output, column, isExtended, r.table) :: Nil


      case _ => Nil
    }

}

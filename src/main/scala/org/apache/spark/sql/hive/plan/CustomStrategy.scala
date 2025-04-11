package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ShowTables, ShowViews}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.{ShowCatalogViews, ShowViewsExec}

object CustomStrategy extends Strategy with Serializable  {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    plan match {

      case ShowCatalogViews(ResolvedNamespace(catalog, ns), pattern, output) =>
        ShowViewsExec(output, catalog.asTableCatalog, ns, pattern) :: Nil
      case _ => Nil
    }

}

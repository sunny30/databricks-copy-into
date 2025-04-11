package org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolveCatalogs, ResolveSessionCatalog, ResolvedNamespace, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin}
import org.apache.spark.sql.delta.util.AnalysisHelper

class ResolveCatalogViews(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {


    case s@ShowCatalogViews(ResolvedNamespace(catalog, namespace),_,_) =>
      s.copy(namespace = ResolvedNamespace(catalog, namespace))

    case pl: LogicalPlan => pl
  }

}

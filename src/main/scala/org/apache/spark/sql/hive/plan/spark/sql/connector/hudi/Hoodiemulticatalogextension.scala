package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Spark session extension for multi-catalog Hudi support.
 * Spark 3.5.0 compatible.
 *
 * Register via:
 *   spark.sql.extensions = com.example.hudi.multicatalog.extension.HoodieMultiCatalogExtension
 *
 * CRITICAL DIFFERENCE from HoodieSparkSessionExtension:
 *   We do NOT inject a MergeIntoTable rewrite rule.
 *   MultiCatalogHudiTable implements SupportsRowLevelOperations, so Spark's built-in
 *   ResolveRowLevelCommands analyzer rule handles MERGE/UPDATE/DELETE natively —
 *   regardless of which catalog the table lives in.
 *
 * We inject only:
 *   1. HudiMultiCatalogOptimizerRule — file/data skipping pushdown stub
 *   2. HudiCallProcedureResolutionRule — CALL compaction/clustering procedure routing
 */
class HoodieMultiCatalogExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { spark =>
      new HudiMultiCatalogOptimizerRule(spark)
    }
    extensions.injectResolutionRule { spark =>
      new HudiCallProcedureResolutionRule(spark)
    }
  }
}

/**
 * Optimizer rule for Hudi-specific scan optimizations.
 *
 * Current: pass-through — HudiScanBuilder.pushFilters handles partition pruning.
 * Future:  inject data skipping predicates from the column stats index when
 *          hoodie.metadata.enable=true. The metadata table exposes column-level
 *          min/max stats that can eliminate entire file groups before listing.
 */
class HudiMultiCatalogOptimizerRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case rel @ DataSourceV2Relation(table: MultiCatalogHudiTable, _, _, _, _) =>
      // Placeholder: inject column stats filter pushdown here when metadata table is enabled
      rel
  }
}

/**
 * Resolution rule for CALL statements targeting Hudi procedures.
 *
 * Hudi procedures: run_compaction, schedule_compaction, run_clustering,
 *                  show_commits, rollback_to_instant, etc.
 *
 * Example usage (once implemented):
 *   CALL lake.system.run_compaction(table => 'lake.analytics.events')
 *
 * These work independently of spark_catalog, routed by catalog prefix in the CALL statement.
 */
class HudiCallProcedureResolutionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
  // TODO: Intercept UnresolvedProcedure and delegate to HoodieProcedures registry
  // guided by the catalog prefix in the procedure identifier
}
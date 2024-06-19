package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class CustomDataSourceAnalyzer(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case DataSourceV2Relation(table:V1Table, _, _, _, _) =>

      val provider = table.v1Table.provider.getOrElse("custom")
      val dataSource = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.v1Table.partitionColumnNames,
          bucketSpec = table.v1Table.bucketSpec,
          className = table.v1Table.provider.get,
          options = table.v1Table.storage.properties,
          catalogTable = Some(table.v1Table))
      if(provider.equalsIgnoreCase("custom")) {
        LogicalRelation(dataSource.resolveRelation(false), table.v1Table)
      }else{
        LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
      }

    //in managed catalog we have to fix this.
    case x@Project(p, child@SubqueryAlias(identifier, child1:DataSourceV2Relation))
      if child1.catalog.isDefined =>
      x.setAnalyzed()
//      child.setAnalyzed()
//      child1.setAnalyzed()
      val table = child1.table.asInstanceOf[V1Table]
      val provider = child1.table.asInstanceOf[V1Table].v1Table.provider.getOrElse("custom")
      val dataSource = DataSource(
        session,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.v1Table.partitionColumnNames,
        bucketSpec = table.v1Table.bucketSpec,
        className = table.v1Table.provider.get,
        options = table.v1Table.storage.properties,
        catalogTable = Some(table.v1Table))
      val relation = if (provider.equalsIgnoreCase("custom")) {
        LogicalRelation(dataSource.resolveRelation(false), table.v1Table)
      } else {
        LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
      }
      val newRelation = relation.copy(output = child1.output, catalogTable = Some(table.v1Table),relation = relation.relation, isStreaming = false)
      val newChild = child.copy(identifier=identifier, child = newRelation)
      val op = x.copy(projectList=p, child = newChild)
      op.resolved
      op.setAnalyzed()

      op
     // child.setAnalyzed()
    //  child

    case p:LogicalPlan => p
  }


}

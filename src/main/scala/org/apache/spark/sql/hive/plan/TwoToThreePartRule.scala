package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.sources.CreatableRelationProvider

class TwoToThreePartRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {


  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp{
    case c@CreateTableCommand(table, ignoreIfExists) => c //needs change
    case cd@CreateDataSourceTableCommand(table,ignoreIfExists) if DDLUtils.isDatasourceTable(table) => cd //needs change
    case ch@CreateHiveTableAsSelectCommand(tableDesc: CatalogTable, query: LogicalPlan, outputColumnNames: Seq[String], mode: SaveMode) => ch
    case cdas@CreateDataSourceTableAsSelectCommand(table: CatalogTable, mode: SaveMode, query: LogicalPlan, outputColumnNames: Seq[String]) => cdas
    case dsw@SaveIntoDataSourceCommand(query: LogicalPlan, dataSource: CreatableRelationProvider, options: Map[String, String], mode: SaveMode)  => dsw



  }

}

package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateDataSourceTableCommand, CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.hive.plan.spark.sql.execution.plan.{CreateCatalogTable, CustomCreateDataSourceTableAsSelectCommand}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}

class TwoToThreePartRule(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  private def getCurrentOrDefaultCatalog:String={
    session.sessionState.catalogManager.currentCatalog.name()
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case c@CreateTableCommand(table, ignoreIfExists) => CreateCatalogTable(getCurrentOrDefaultCatalog,table, ignoreIfExists) //needs change

    case cd@CreateDataSourceTableCommand(table, ignoreIfExists) if DDLUtils.isDatasourceTable(table) => CreateCatalogTable(getCurrentOrDefaultCatalog,table, ignoreIfExists)//needs change

    case ch@CreateHiveTableAsSelectCommand(tableDesc: CatalogTable, query: LogicalPlan, outputColumnNames: Seq[String], mode: SaveMode) =>
      val newTableDesc = getNewTableWithFileFormatProperty(tableDesc)
      CustomCreateDataSourceTableAsSelectCommand(getCurrentOrDefaultCatalog,newTableDesc,mode,query,outputColumnNames)

    case cdas@CreateDataSourceTableAsSelectCommand(table: CatalogTable, mode: SaveMode, query: LogicalPlan, outputColumnNames: Seq[String]) => CustomCreateDataSourceTableAsSelectCommand(getCurrentOrDefaultCatalog,table,mode,query,outputColumnNames)

    case dsw@SaveIntoDataSourceCommand(query: LogicalPlan, dataSource: CreatableRelationProvider, options: Map[String, String], mode: SaveMode) => dsw

    case inds@InsertIntoDataSourceCommand(logicalRelation: LogicalRelation, query: LogicalPlan, overwrite: Boolean) => inds

    case lrds@LogicalRelation(r: BaseRelation, _, table: Option[CatalogTable], false) if table.isDefined => lrds

    case hiveTableRelation@HiveTableRelation(tableMeta: CatalogTable, dataCols: Seq[AttributeReference],
    partitionCols: Seq[AttributeReference], tableStats: Option[Statistics], _) => hiveTableRelation

    case insertIntoHiveTable: InsertIntoHiveTable => insertIntoHiveTable

    case insertIntoHadoopFSRelationCommand@InsertIntoHadoopFsRelationCommand(outputPath, staticPartitions, ifPartitionNotExists, partitionColumns, bucketSpec, fileFormat, options,
    query, mode, catalogTable, fileIndex, outputColumnNames) if catalogTable.isDefined  => insertIntoHadoopFSRelationCommand

    case plan: LogicalPlan =>
      println("Two to three part name plan logic "+ plan.toString())
      plan


  }


  def getNewTableWithFileFormatProperty(table: CatalogTable): CatalogTable = {
    val fileFormat = table.provider match {
      case Some("hive") =>
        table.storage.serde match {
          case Some(v) =>
            if (v.contains("parquet"))
              "parquet"
            else if (v.contains("orc"))
              "orc"
            else if (v.contains("avro"))
              "avro"
            else if (v.contains("json") || v.contains("Json"))
              "json"
            else {
              "textfile"
            }
        }
      case Some(v) => v
      case _ => throw new IllegalArgumentException("Provider format is missing")
    }

    val existingProps = table.storage.properties
    val newStorageProps = existingProps ++ Map("fileformat" -> fileFormat)
    val storage = table.storage.copy(properties = newStorageProps)
    val existingTblProps = table.properties
    val newTblProps = existingTblProps ++ Map("fileformat" -> fileFormat)
    table.copy(properties = newTblProps, storage = storage)

  }

}

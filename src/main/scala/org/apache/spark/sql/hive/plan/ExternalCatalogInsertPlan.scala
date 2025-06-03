package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.InsertableRelation

case class ExternalCatalogInsertPlan(in: InsertIntoDataSourceCommand)
  extends LeafRunnableCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(in)


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val isExternalCatalogSource = in.logicalRelation.catalogTable.isDefined &&
      in.logicalRelation.catalogTable.get.provider.get.equalsIgnoreCase("custom")


    try {

      in.run(sparkSession)
    } catch {
      case e: Exception => throw e
    }

    if (isExternalCatalogSource) {
      refreshTable(in.logicalRelation.catalogTable.get)
    }

    Seq.empty[Row]
  }

  def refreshTable(ct: CatalogTable): Unit = {
    if (ct.identifier.catalog.isDefined && ct.identifier.database.isDefined) {
      val catalogName = ct.identifier.catalog.get
      val schemaName = ct.identifier.database.get
      val tableName = ct.identifier.table
      val qualifiedName = catalogName + "." + schemaName + "." + tableName
    } else {
      println("Unable to refresh")
    }
  }


}

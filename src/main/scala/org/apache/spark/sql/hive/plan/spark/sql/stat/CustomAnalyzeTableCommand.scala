package org.apache.spark.sql.hive.plan.spark.sql.stat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.command.{CommandUtils, LeafRunnableCommand}

case class CustomAnalyzeTableCommand(tableIdent: TableIdentifier, plugin: CatalogPlugin) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    AnalyzeCommandUtil.analyzeTable(SparkSession.active, tableIdent, plugin)
    Seq.empty[Row]
  }
}

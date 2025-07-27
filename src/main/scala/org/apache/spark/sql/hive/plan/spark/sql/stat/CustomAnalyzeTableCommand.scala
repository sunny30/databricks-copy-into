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



case class CustomAnalyzeColumnCommand(
                                 tableIdent: TableIdentifier,
                                 columnNames: Option[Seq[String]],
                                 allColumns: Boolean) extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
      "mutually exclusive. Only one of them should be specified.")
    val sessionState = sparkSession.sessionState

    AnalyzeCommandUtil.analyzeColumnInCatalog(sparkSession, tableIdent.catalog.getOrElse("spark_catalog"), tableIdent.database.getOrElse("default"), tableIdent.table, columnNames, allColumns)

    Seq.empty[Row]
  }
}



package org.apache.spark.sql.hive.plan.spark.sql.delta.commad

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.commands.{ConvertToDeltaCommand, DeltaCommand}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import scala.jdk.CollectionConverters.mapAsScalaMapConverter
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
import org.apache.spark.sql.types.StructType

case class CustomConvertToDeltaCommand(convertToDeltaCommand: ConvertToDeltaCommand) extends LeafRunnableCommand with DeltaCommand{
  override def run(spark: SparkSession): Seq[Row] = {

    val tableIdentifier = convertToDeltaCommand.tableIdentifier
    var newTblIdent = convertToDeltaCommand.tableIdentifier
    var partitionStruct = convertToDeltaCommand.partitionSchema

    if (tableIdentifier.catalog.isDefined) {
      val tc = spark.sessionState.catalogManager.catalog(tableIdentifier.catalog.get).asTableCatalog
      val metadata = tc.loadTable(
        Identifier.of(Seq(tableIdentifier.database.getOrElse("default")).toArray,
          tableIdentifier.table))
      metadata.partitioning()
      val tblProps = metadata.properties().asScala
      val location = tblProps.get(TableCatalog.PROP_LOCATION)
      val provider = tblProps.get(TableCatalog.PROP_PROVIDER)
      val finalProvider = if (provider.getOrElse("parquet").equalsIgnoreCase("hive")) {
        Some(tblProps.getOrElse("fileformat", "csv"))
      } else {
        provider
      }
      newTblIdent = TableIdentifier(location.get, database = finalProvider, None)

      val (partitionColumns, maybeBucketSpec) = metadata.partitioning().toSeq.convertTransforms
      partitionStruct = if (partitionColumns.isEmpty) {
        None
      } else {
        Some(getPartitionSchema(metadata, partitionColumns))
      }
      val cvt = convertToDeltaCommand.copy(tableIdentifier = newTblIdent, partitionSchema = partitionStruct)
      val res = cvt.run(spark)
      if(spark.conf.get("spark.sql.test.env").equalsIgnoreCase("true")){
        tc.alterTable(
          Identifier.of(Seq(tableIdentifier.database.getOrElse("default")).toArray,
            tableIdentifier.table),
          TableChange.setProperty("provider", "delta")

        )

        tc.alterTable(
          Identifier.of(Seq(tableIdentifier.database.getOrElse("default")).toArray,
            tableIdentifier.table),
          TableChange.setProperty("spark.sql.sources.provider", "delta")

        )
      }else {
        tc.alterTable(
          Identifier.of(Seq(tableIdentifier.database.getOrElse("default")).toArray,
            tableIdentifier.table),
          TableChange.setProperty("spark.sql.sources.provider", "delta")

        )
      }
      res
    }else{
      convertToDeltaCommand.run(spark)
    }

  }

  private def getPartitionSchema(tbl: Table, partitionCols: Seq[String]): StructType = {
    val partitionFields = tbl.schema().filter(f => partitionCols.toSet.contains(f.name)).toSeq
    StructType(partitionFields)
  }
}

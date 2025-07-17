package org.apache.spark.sql.hive.plan.spark.sql.connector


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVDataSourceV2, CSVTable}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonDataSourceV2, JsonTable}
import org.apache.spark.sql.execution.datasources.v2.orc.{OrcDataSourceV2, OrcTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.v2.avro.{AvroDataSourceV2, AvroTable}

import java.util

case class V2CustomTable(name: String,
                         sparkSession: SparkSession,
                         options: CaseInsensitiveStringMap,
                         catalogTable: CatalogTable) extends SupportsRead with Table{
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val provider = catalogTable.provider.getOrElse("parquet")

    val fileTable = provider.toLowerCase match {
      case "parquet" => new ParquetDataSourceV2().getTable(options).asInstanceOf[ParquetTable]
      case "orc" => new OrcDataSourceV2().getTable(options).asInstanceOf[OrcTable]
      case "avro" => new AvroDataSourceV2().getTable(options).asInstanceOf[AvroTable]
      case "csv" => new CSVDataSourceV2().getTable(options).asInstanceOf[CSVTable]
      case "json" => new JsonDataSourceV2().getTable(options).asInstanceOf[JsonTable]
    }

    val fileIndex = fileTable.fileIndex
    val dataSchema = fileTable.dataSchema
    val readSchema = fileTable.schema

    V2CustomTableScanBuilder(provider, sparkSession, fileIndex,readSchema, dataSchema, options)


  }

  override def schema(): StructType = catalogTable.schema

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.allOf(classOf[TableCapability])
}

package org.apache.spark.sql.hive.plan.spark.sql.connector


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2.{FileDataSourceV2, FileTable}
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVDataSourceV2, CSVTable}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonDataSourceV2, JsonTable}
import org.apache.spark.sql.execution.datasources.v2.orc.{OrcDataSourceV2, OrcTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.execution.datasources.v2.text.{TextDataSourceV2, TextTable}
import org.apache.spark.sql.hive.classloader.reflection.ReflectionUtil
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.v2.avro.{AvroDataSourceV2, AvroTable}
import org.apache.spark.util.Utils

import java.util
import java.util.ServiceLoader

case class V2CustomTable(name: String,
                         sparkSession: SparkSession,
                         options: CaseInsensitiveStringMap,
                         catalogTable: CatalogTable) extends SupportsRead with Table{

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val provider = if(catalogTable.provider.get.equalsIgnoreCase("hive")) {
      catalogTable.storage.properties("fileformat").toLowerCase

    }else{
      catalogTable.provider.getOrElse("parquet")
    }
    val multiPartName  = Seq(catalogTable.identifier.catalog.getOrElse("spark_catalog"), catalogTable.identifier.database.getOrElse("default"), catalogTable.identifier.table)

    if(provider.toLowerCase.equalsIgnoreCase("custom")){
      val clazzName = "org.apache.spark.sql.hive.plan.spark.sql.connector.custom.CustomTable"
      val methodName = "newScanBuilder"
      return ReflectionUtil.reflectScanBuilder(clazzName, methodName, schema, options)
    }

    val fileTable = provider.toLowerCase match {
      case "parquet" => new ParquetDataSourceV2().getTable(options).asInstanceOf[ParquetTable]
      case "orc" => new OrcDataSourceV2().getTable(options).asInstanceOf[OrcTable]
      case "avro" => new AvroDataSourceV2().getTable(options).asInstanceOf[AvroTable]
      case "csv" => new CSVDataSourceV2().getTable(options).asInstanceOf[CSVTable]
      case "json" => new JsonDataSourceV2().getTable(options).asInstanceOf[JsonTable]
      case "text" => new TextDataSourceV2().getTable(options).asInstanceOf[TextTable]
      case "textfile" => new CSVDataSourceV2().getTable(options).asInstanceOf[CSVTable]

    }

    var fileIndex = fileTable.fileIndex
    val dataSchema = catalogTable.dataSchema
    val readSchema = catalogTable.schema

    val paths = getPaths(options)
    val customFileTable = new V2CustomFileTable(sparkSession = sparkSession, options = options, paths = paths, userSpecifiedSchema = Some(readSchema), fileTable = fileTable, catalogTable = catalogTable)
    //fileIndex = customFileTable.fileIndex
    V2CustomTableScanBuilder(multiPartName,provider, sparkSession, fileIndex,readSchema, dataSchema, options)


  }

  protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(map.get("paths")).map { pathStr =>
      FileDataSourceV2.readPathsToSeq(pathStr)
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  override def schema(): StructType = catalogTable.schema

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.allOf(classOf[TableCapability])

  def mapHiveCSVPropertiesToSparkOption(ct: CatalogTable, fileFormat: FileFormat): Map[String, String] = {
    var tblProps = ct.properties

    //tblProps.
    if (fileFormat.isInstanceOf[CSVFileFormat]) {
      if (!tblProps.contains("option.delimiter")) {
        tblProps = tblProps ++ Map("delimiter" -> tblProps.getOrElse("field.delim", ","))
      }

      if (!tblProps.contains("option.quote")) {
        tblProps = tblProps ++ Map("quote" -> tblProps.getOrElse("quoteChar", '\"'.toString))
      }

      if (!tblProps.contains("option.escape")) {
        tblProps = tblProps ++ Map("escape" -> tblProps.getOrElse("escape.delim", '\\'.toString))
      }

      if (!tblProps.contains("option.header")) {
        //tblProps.getOrElse("skip")
        tblProps = tblProps ++ Map("header" -> tblProps.getOrElse("hasheaders", "false"))
      }

      if (!tblProps.contains("option.lineSep")) {
        //tblProps.getOrElse("skip")
        tblProps = tblProps ++ Map("lineSep" -> tblProps.getOrElse("recorddelimiter", "\n"))
      }

      tblProps
    } else {
      tblProps
    }

  }
}

private object FileDataSourceV2 {
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  def readPathsToSeq(paths: String): Seq[String] =
    objectMapper.readValue(paths, classOf[Seq[String]])
}

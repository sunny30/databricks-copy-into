package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, SubqueryAlias, TableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, removeInternalMetadata}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, V1Table}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.asJavaIterableConverter

case class CustomOptimizedPlan(spark:SparkSession) extends Rule[LogicalPlan] {

  protected def getV2Columns(schema: StructType, forceNullable: Boolean): Array[Column] = {
    val rawSchema = CharVarcharUtils.getRawSchema(removeInternalMetadata(schema), conf)
    val tableSchema = if (forceNullable) rawSchema.asNullable else rawSchema
    CatalogV2Util.structTypeToV2Columns(tableSchema)
  }

  def getFileFormat(formatName: String): FileFormat = {
    formatName.toLowerCase match {
      case "csv" => new CSVFileFormat
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "orc" => new OrcFileFormat
      case "avro" => new AvroFileFormat
      case _ => new CSVFileFormat
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case ctas@CreateTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
    options, ifNotExists, true) =>

      val properties = CatalogV2Util.convertTableProperties(tableSpec)

////        ident,
////        getV2Columns(query.schema,false),
////        parts.toArray,
////        properties.asJava
////      )
      val outputs = query.schema.map(s=>s.name)
     // CustomDataSourceAsSelectCommand(catalog.asTableCatalog,table.asInstanceOf[V1Table].v1Table,SaveMode.ErrorIfExists,query,outputs)
      if(properties.getOrElse("provider", "hive").equalsIgnoreCase("delta")){
        plan
      }else {
        val table = catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray,mapAsJavaMap(properties))
        InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(table.asInstanceOf[V1Table].v1Table.storage.locationUri.get.toString),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty[Attribute],
          bucketSpec = None,
          fileFormat = getFileFormat(table.asInstanceOf[V1Table].v1Table.provider.getOrElse("csv")),
          Map.empty,
          query = query.asInstanceOf[SubqueryAlias].child,
          SaveMode.Append,
          None,
          None,
          query.output.map(_.name)
        )
      }


    //ctas
    case p: LogicalPlan => plan
  }
}

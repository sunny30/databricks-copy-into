package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{AliasIdentifier, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect, SubqueryAlias, TableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, removeInternalMetadata}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, StagingTableCatalog, Table, TableCatalog, V1Table}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{AtomicReplaceTableAsSelectExec, DataSourceV2Relation, ReplaceTableAsSelectExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
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

  private def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(spark.sharedState.conf.get(WAREHOUSE_PATH),
      location, spark.sharedState.hadoopConf)
  }
  private def qualifyLocInTableSpec(tableSpec: TableSpec): TableSpec = {
    tableSpec.withNewLocation(tableSpec.location.map(makeQualifiedDBObjectPath(_)))
  }

  private def invalidateCache(catalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    spark.sharedState.cacheManager.uncacheQuery(spark, v2Relation, cascade = true)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(plan.toString())
    plan match {

      case rtas@ReplaceTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
      _, _, _) =>
        println("Inside RTAS")
        val properties = CatalogV2Util.convertTableProperties(tableSpec)
        val projectPlan = EliminateSubqueryAliases(query)
        val outputs = query.schema.map(s => s.name)

        if (properties.getOrElse("provider", "hive").equalsIgnoreCase("delta")) {
          println("Inside delta plan block")
          plan
        }else {
          if(catalog.asTableCatalog.tableExists(ident)){
            catalog.asTableCatalog.dropTable(ident)
          }
          val table = catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray, mapAsJavaMap(properties))
          InsertIntoHadoopFsRelationCommand(
            outputPath = new Path(table.asInstanceOf[V1Table].v1Table.storage.locationUri.get.toString),
            staticPartitions = Map.empty,
            ifPartitionNotExists = false,
            partitionColumns = Seq.empty[Attribute],
            bucketSpec = None,
            fileFormat = getFileFormat(table.asInstanceOf[V1Table].v1Table.provider.getOrElse("csv")),
            Map.empty,
            query = projectPlan,
            SaveMode.Overwrite,
            None,
            None,
            query.output.map(_.name)
          )
        }


      case ctas@CreateTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
      _, _, _) =>
        println("Inside CTAS")
        val properties = CatalogV2Util.convertTableProperties(tableSpec)
        val projectPlan = EliminateSubqueryAliases(query)

        ////        ident,
        ////        getV2Columns(query.schema,false),
        ////        parts.toArray,
        ////        properties.asJava
        ////      )
        val outputs = query.schema.map(s => s.name)
        // CustomDataSourceAsSelectCommand(catalog.asTableCatalog,table.asInstanceOf[V1Table].v1Table,SaveMode.ErrorIfExists,query,outputs)
        if (properties.getOrElse("provider", "hive").equalsIgnoreCase("delta")) {
//          val newQuery = spark.sessionState.analyzer.executeAndCheck(query, new QueryPlanningTracker())
//          newQuery.setAnalyzed()
//          println("Inside delta block")
//          ctas.setAnalyzed()
//          ctas.copy(query = newQuery)
          plan
        } else {
          val table = catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray, mapAsJavaMap(properties))
          InsertIntoHadoopFsRelationCommand(
            outputPath = new Path(table.asInstanceOf[V1Table].v1Table.storage.locationUri.get.toString),
            staticPartitions = Map.empty,
            ifPartitionNotExists = false,
            partitionColumns = Seq.empty[Attribute],
            bucketSpec = None,
            fileFormat = getFileFormat(table.asInstanceOf[V1Table].v1Table.provider.getOrElse("csv")),
            Map.empty,
            query = projectPlan,
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
}

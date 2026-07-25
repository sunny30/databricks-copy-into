package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{AliasIdentifier, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect, SubqueryAlias, TableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, removeInternalMetadata}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, CatalogV2Util, Column, Identifier, StagingTableCatalog, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{AtomicReplaceTableAsSelectExec, DataSourceV2Relation, ReplaceTableAsSelectExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.dynamicpruning.CleanupDynamicPruningFilters
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.hive.plan.listener.ListenerUtil
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.asJavaIterableConverter

case class CustomOptimizedPlan(spark:SparkSession) extends Rule[LogicalPlan] {

  protected def getV2Columns(schema: StructType, forceNullable: Boolean): Array[Column] = {
    val rawSchema = CharVarcharUtils.getRawSchema(removeInternalMetadata(schema), conf)
    val tableSchema = if (forceNullable) rawSchema.asNullable else rawSchema
    CatalogV2Util.structTypeToV2Columns(tableSchema)
  }

  def getPartitionAttributeFromV2Table(query:LogicalPlan, table:Table):Seq[Attribute] = {
    val ct = table.asInstanceOf[V2Table].v1Table
    // ctas.partitioning.map(t => )
    val ps = query.resolve(
      ct.partitionSchema, spark.sessionState.analyzer.resolver)
    ps
  }

  def getFileFormat(formatName: String): FileFormat = {
    formatName.toLowerCase match {
      case "csv" => new CSVFileFormat
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
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


  def shouldDropExistingTable(table:Table,targetProvider:String):Boolean ={
    (table,targetProvider.toLowerCase) match {
      case (t:V2Table,_) => true
      case (t:DeltaTableV2,"delta") => false
      case (t:DeltaTableV2,_) => true
      case (t:SparkTable , _)=> false

      case (_,_) => true
    }
  }


  def targetTableProvider(tableSpec: TableSpec, existingProvider: String): String = {
    val defaultDatasource = conf.defaultDataSourceName
    if(existingProvider.equalsIgnoreCase("iceberg")) {
      return existingProvider
    }
    tableSpec.provider match {
      case Some(v) => v
      case None =>
        if (existingProvider.equalsIgnoreCase("delta") || existingProvider.equalsIgnoreCase("iceberg")) {
          existingProvider
        } else {
          defaultDatasource
        }

    }

  }






  def getExistingTable(catalog:CatalogPlugin, identifier: Identifier):Table ={
    catalog.asTableCatalog.loadTable(identifier)
  }


  def getActualProvider(catalog:CatalogPlugin, identifier: Identifier, tableSpec: TableSpec):String = {
    if(catalog.asTableCatalog.tableExists(identifier)){
     // catalog.name()
      catalog.asTableCatalog.loadTable(identifier) match {
        case v2Table: V2Table => v2Table.v1Table.provider.get
        case deltaTableV2: DeltaTableV2 => deltaTableV2.v1Table.provider.get
        case sparkTable: SparkTable => "iceberg"
        case _ => throw new IllegalArgumentException("not a valid data format table")
      }
    }else{
      val properties = CatalogV2Util.convertTableProperties(tableSpec)
      val defaultDatasource = conf.defaultDataSourceName
      properties.getOrElse("provider", defaultDatasource)
    }
  }

//only applicable to V2Table
  def getOldTableProps(catalog:CatalogPlugin, identifier: Identifier, tableSpec: TableSpec):Map[String,String]={
    if (catalog.asTableCatalog.tableExists(identifier)) {
      // catalog.name()
      catalog.asTableCatalog.loadTable(identifier).asInstanceOf[V2Table].v1Table.properties
    } else {
      val properties = CatalogV2Util.convertTableProperties(tableSpec)
      properties
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    println(plan.toString())
    plan match {

      case rtas@ReplaceTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
      _, _, _) =>
        println("****Inside RTAS****")
        var properties = CatalogV2Util.convertTableProperties(tableSpec)
        val qe = spark.sessionState.executePlan(query, CommandExecutionMode.NON_ROOT)
        val dynamicPartitonPruningExists = qe.optimizedPlan.exists(pl => pl.expressions.exists(e => e.containsAnyPattern(DYNAMIC_PRUNING_SUBQUERY)))


      //  spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
        val writePlan = if (dynamicPartitonPruningExists) {
          qe.commandExecuted
        } else {
          qe.optimizedPlan
        }
        val outputs = query.schema.map(s => s.name)
        val existingProvider = getActualProvider(catalog,ident,tableSpec)
        val targetProvider = targetTableProvider(tableSpec, existingProvider)
        val tableExists = catalog.asTableCatalog.tableExists(ident)
        var table: Table = if (tableExists) getExistingTable(catalog, ident) else null
        val dropAndCreateTable = tableExists && shouldDropExistingTable(table, targetProvider)
        val newTableSpec = tableSpec.copy(provider = Some(targetProvider))
        var newTableProperties = CatalogV2Util.convertTableProperties(newTableSpec)

        if (dropAndCreateTable) {
          catalog.asTableCatalog.dropTable(ident)
          table = catalog.asTableCatalog.createTable(
            ident, query.schema, parts.toArray, mapAsJavaMap(newTableProperties))
        } else if (!tableExists && !targetProvider.equalsIgnoreCase("delta")
          && !targetProvider.equalsIgnoreCase("iceberg")) {
          // orCreate = true and table doesn't exist — create fresh
          table = catalog.asTableCatalog.createTable(
            ident, query.schema, parts.toArray, mapAsJavaMap(newTableProperties))
        }
        /**Delta External we have to see later**/
        if (targetProvider.equalsIgnoreCase("delta")) {

          println("Inside delta or custom datasource plan block")
          rtas.copy(query = writePlan, tableSpec = newTableSpec)

        }else if(targetProvider.equalsIgnoreCase("custom")){
          properties = getOldTableProps(catalog,ident,tableSpec)
          val newTableSpec = tableSpec.copy(properties = properties,provider = Some(existingProvider))
          rtas.copy(tableSpec = newTableSpec,query = writePlan)
        }else if(targetProvider.equalsIgnoreCase("iceberg")){
          rtas.copy(query = writePlan, tableSpec = newTableSpec)
        }else {
          val ps = getPartitionAttributeFromV2Table(writePlan, table)
          InsertIntoHadoopFsRelationCommand(
            outputPath = new Path(table.asInstanceOf[V2Table].v1Table.storage.locationUri.get.toString),
            staticPartitions = Map.empty,
            ifPartitionNotExists = false,
            partitionColumns = ps,
            bucketSpec = None,
            fileFormat = getFileFormat(table.asInstanceOf[V2Table].v1Table.provider.getOrElse("csv")),
            Map.empty,
            query = writePlan,
            SaveMode.Overwrite,
            None,
            None,
            outputs
          )
        }


      case ctas@CreateTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
      _, _, _) =>
        println("****Inside CTAS****")

        var properties = CatalogV2Util.convertTableProperties(tableSpec)
       // val projectPlan = EliminateSubqueryAliases(query)
       // spark.sessionState.
        val qe = spark.sessionState.executePlan(query,CommandExecutionMode.NON_ROOT)
        val dynamicPartitonPruningExists = qe.optimizedPlan.exists(pl => pl.expressions.exists(e=>e.containsAnyPattern(DYNAMIC_PRUNING_SUBQUERY)))


      println("results inside cts "+ListenerUtil.getSQLTextIfExists(ctas))
      println("result inside ctas query "+ ListenerUtil.getSQLTextIfExists(ctas))




      //  println("Plan is optimized: "+ qe.optimizedPlan.ass)
        val writePlan = if(dynamicPartitonPruningExists){
          qe.commandExecuted
        }else{
          qe.optimizedPlan
        }

//        val writePlan = spark.sessionState.optimizer.execute(projectPlan)
//        val analyzedWritePlan = spark.sessionState.analyzer.execute(writePlan)
//        spark.sessionState.analyzer.executeAndCheck(analyzedWritePlan, new QueryPlanningTracker())
//        analyzedWritePlan.setAnalyzed()

       // query.schema.toDDL
        //val outputs = query.schema.map(s => s.name)
        val providerValue = getActualProvider(catalog,ident,tableSpec)

        if (providerValue.equalsIgnoreCase("delta")) {
          if (catalog.asTableCatalog.tableExists(ident)) {
            catalog.asTableCatalog.dropTable(ident)
          }
       // One has to drop Delta CTAS will create delta table
          ctas.copy(query = writePlan)
        } else if (providerValue.equalsIgnoreCase("iceberg")) {
          ctas.copy(query = writePlan)
        }else {
          val table = catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray, mapAsJavaMap(properties))
          val ps = getPartitionAttributeFromV2Table(writePlan,table)
          InsertIntoHadoopFsRelationCommand(
            outputPath = new Path(table.asInstanceOf[V2Table].v1Table.storage.locationUri.get.toString),
            staticPartitions = Map.empty,
            ifPartitionNotExists = false,
            partitionColumns = ps,
            bucketSpec = None,
            fileFormat = getFileFormat(table.asInstanceOf[V2Table].v1Table.provider.getOrElse("csv")),
            Map.empty,
            query = writePlan,
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

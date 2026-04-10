package org.apache.spark.sql.hive.plan.spark.sql.execution.plan

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.arrow.ArrowFileFormat
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, Table, TableSchemaChangeCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}

import scala.collection.JavaConverters._
import java.net.URI
case class CreateCatalogTable(catalogName: String, table: CatalogTable, ignoreIfExists: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    if (sessionState.catalog.tableExists(table.identifier)) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw QueryCompilationErrors.tableAlreadyExistsError(table.identifier.unquotedString)
      }
    }else{
      val tableURI = getTablePathTable(sparkSession)
      val newTable = table.copy(storage = table.storage.copy(locationUri = tableURI))

      if (SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")) {
        val dbPath = sparkSession.sessionState.catalog.getDatabaseMetadata(table.database).locationUri
        SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[SupportsNamespaces].createNamespace(Array(table.database), Map("location" -> dbPath.toString).asJava)
        sessionState.catalog.createTable(newTable, ignoreIfExists = false)
      }
      SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableSchemaChangeCatalog].registerTableInMetastore(newTable, ignoreIfExists)

      Seq.empty[Row]

    }
  }


  def getTablePathTable(sparkSession: SparkSession):Option[URI]={

    val dbPath = sparkSession.sessionState.catalog.getDatabaseMetadata(table.database).locationUri
    val dbStringPath = if (dbPath.toString.endsWith("/")) {
      dbPath.toString
    } else {
      dbPath.toString + "/"
    }
    var location = table.storage.locationUri
    location = location match {
      case None =>
        Some(CatalogUtils.stringToURI(dbStringPath + table.identifier.table))
      case Some(v) => Some(v)

    }
    location
  }
}


case class CustomCreateDataSourceTableAsSelectCommand(
                                                 catalogName: String,
                                                 table: CatalogTable,
                                                 mode: SaveMode,
                                                 query: LogicalPlan,
                                                 outputColumnNames: Seq[String])
  extends LeafRunnableCommand {
  //assert(query.resolved)

  override def innerChildren: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val qe = sparkSession.sessionState.executePlan(query, CommandExecutionMode.NON_ROOT)
    val dynamicPartitonPruningExists = qe.optimizedPlan.exists(pl => pl.expressions.exists(e => e.containsAnyPattern(DYNAMIC_PRUNING_SUBQUERY)))



    //  spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
    val writePlan = if (dynamicPartitonPruningExists) {
      qe.commandExecuted
    } else {
      qe.optimizedPlan
    }
    val tableCatalog = sparkSession.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val ident = Identifier.of(Seq(table.identifier.database.getOrElse("default")).toArray, table.identifier.table)
    val tableExists = tableCatalog.tableExists(ident)
    if(tableExists && mode == SaveMode.ErrorIfExists){
      throw QueryCompilationErrors.tableAlreadyExistsError(table.identifier.quotedString)
    }

    if (mode == SaveMode.Ignore) {
      if (tableExists) {
        return Seq.empty[Row]
      }
    }


    val outPutPath = if(tableExists){
      table.storage.locationUri.get
    }else{
      if(table.storage.locationUri.isDefined){
        table.storage.locationUri.get
      }else {
        sparkSession.sessionState.catalog.defaultTablePath(table.identifier)
      }
    }
    val (schema, outputColumnNames, partitionColumns) = if(mode == SaveMode.Overwrite){
      if(tableExists)
        tableCatalog.dropTable(ident)
      (writePlan.schema, writePlan.output.map(at=>at.name), table.partitionColumnNames)
    }else if(mode == SaveMode.Append){

      if(tableExists){
        (table.schema, table.schema.map(f=>f.name), table.partitionColumnNames)
      }else{
        (writePlan.schema, writePlan.output.map(at=>at.name), table.partitionColumnNames)
      }
    }else{
      if(tableExists){
        throw QueryCompilationErrors.tableAlreadyExistsError(table.identifier.quotedString)
      }else{
        (writePlan.schema, writePlan.output.map(at=>at.name), table.partitionColumnNames)
      }
    }

    val ps = if(!tableExists) {
      val newTable = table.copy(schema = schema,storage = table.storage.copy(locationUri = Some(outPutPath)))
      CreateCatalogTable(catalogName,newTable, false).run(sparkSession)
      getPartitionAttributeFromTable(writePlan,newTable,sparkSession)
    }else{
      getPartitionAttributeFromTable(writePlan, table, sparkSession)
    }

    val fileFormat = if (table.provider.getOrElse("csv").equalsIgnoreCase("hive")) {
      getHiveTableFileFormat(table)
    } else {
      getFileFormat(table.provider.get)
    }


    val newMode = if(mode == SaveMode.ErrorIfExists){
      SaveMode.Append
    }else{
      mode
    }
    InsertIntoHadoopFsRelationCommand(
      outputPath = new Path(outPutPath.toString),
      staticPartitions = Map.empty,
      ifPartitionNotExists = false,
      partitionColumns = ps,
      bucketSpec = None,
      fileFormat = fileFormat,
      table.storage.properties,
      query = writePlan,
      newMode,
      None,
      None,
      outputColumnNames
    ).run(sparkSession,qe.sparkPlan)

    Seq.empty[Row]
  }

  def getHiveTableFileFormat(table: CatalogTable): FileFormat = {
    table.storage.properties("fileformat").toLowerCase match {
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "csv" => new CSVFileFormat
      case "avro" => new AvroFileFormat
      case "json" => new JsonFileFormat
      case "text" => new CSVFileFormat
      case "arrow" => new ArrowFileFormat
      case "_" => throw new IllegalAccessException("invalid format")
    }
  }

  def getFileFormat(formatName: String): FileFormat = {
    formatName.toLowerCase match {
      case "csv" => new CSVFileFormat
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "orc" => new OrcFileFormat
      case "avro" => new AvroFileFormat
      case "json" => new JsonFileFormat
      case "arrow" => new ArrowFileFormat
      case _ => new CSVFileFormat
    }
  }

    def getPartitionAttributeFromTable(query: LogicalPlan, table: CatalogTable, sparkSession: SparkSession): Seq[Attribute] = {

      // ctas.partitioning.map(t => )
      val ps = query.resolve(
        table.partitionSchema, sparkSession.sessionState.analyzer.resolver)
      ps

    }



}

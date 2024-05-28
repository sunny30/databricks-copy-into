//package org.apache.spark.sql.hive.plan
//
//import org.apache.hadoop.fs.Path
//import org.apache.spark.sql.avro.AvroFileFormat
//import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
//import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
//import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
//import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, removeInternalMetadata}
//import org.apache.spark.sql.connector.catalog.TableCatalog
//import org.apache.spark.sql.errors.QueryCompilationErrors
//import org.apache.spark.sql.execution.CommandExecutionMode
//import org.apache.spark.sql.execution.command.{CommandUtils, DataWritingCommand, LeafRunnableCommand, RepairTableCommand}
//import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
//import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
//import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, HadoopFsRelation, InsertIntoHadoopFsRelationCommand}
//import org.apache.spark.sql.hive.orc.OrcFileFormat
//import org.apache.spark.sql.sources.BaseRelation
//
//import java.net.URI
//
//
////case class CreateDataSourceTableAsSelectCommand(
////                                                 table: CatalogTable,
////                                                 mode: SaveMode,
////                                                 query: LogicalPlan,
////                                                 outputColumnNames: Seq[String])
////  extends LeafRunnableCommand {
////  assert(query.resolved)
////  override def innerChildren: Seq[LogicalPlan] = query :: Nil
////
////  override def run(sparkSession: SparkSession): Seq[Row] = {
////    assert(table.tableType != CatalogTableType.VIEW)
////    assert(table.provider.isDefined)
////
////    val sessionState = sparkSession.sessionState
////    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
////    val tableIdentWithDB = table.identifier.copy(database = Some(db))
////    val tableName = tableIdentWithDB.unquotedString
////
////    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
////      assert(mode != SaveMode.Overwrite,
////        s"Expect the table $tableName has been dropped when the save mode is Overwrite")
////
////      if (mode == SaveMode.ErrorIfExists) {
////        throw QueryCompilationErrors.tableAlreadyExistsError(tableName)
////      }
////      if (mode == SaveMode.Ignore) {
////        // Since the table already exists and the save mode is Ignore, we will just return.
////        return Seq.empty
////      }
////
////      saveDataIntoTable(
////        sparkSession, table, table.storage.locationUri, SaveMode.Append, tableExists = true)
////    } else {
////      table.storage.locationUri.foreach { p =>
////        DataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
////      }
////      assert(table.schema.isEmpty)
////      sparkSession.sessionState.catalog.validateTableLocation(table)
////      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
////        Some(sessionState.catalog.defaultTablePath(table.identifier))
////      } else {
////        table.storage.locationUri
////      }
////      val result = saveDataIntoTable(
////        sparkSession, table, tableLocation, SaveMode.Overwrite, tableExists = false)
////      val tableSchema = CharVarcharUtils.getRawSchema(
////        removeInternalMetadata(result.schema), sessionState.conf)
////      val newTable = table.copy(
////        storage = table.storage.copy(locationUri = tableLocation),
////        // We will use the schema of resolved.relation as the schema of the table (instead of
////        // the schema of df). It is important since the nullability may be changed by the relation
////        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
////        schema = tableSchema)
////      // Table location is already validated. No need to check it again during table creation.
////      sessionState.catalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
////
////      result match {
////        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
////          sparkSession.sqlContext.conf.manageFilesourcePartitions =>
////          // Need to recover partitions into the metastore so our saved data is visible.
////          sessionState.executePlan(RepairTableCommand(
////            table.identifier,
////            enableAddPartitions = true,
////            enableDropPartitions = false), CommandExecutionMode.SKIP).toRdd
////        case _ =>
////      }
////    }
////
////    CommandUtils.updateTableStats(sparkSession, table)
////
////    Seq.empty[Row]
////  }
////
////  private def saveDataIntoTable(
////                                 session: SparkSession,
////                                 table: CatalogTable,
////                                 tableLocation: Option[URI],
////                                 mode: SaveMode,
////                                 tableExists: Boolean): BaseRelation = {
////    // Create the relation based on the input logical plan: `query`.
////    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
////    val dataSource = DataSource(
////      session,
////      className = table.provider.get,
////      partitionColumns = table.partitionColumnNames,
////      bucketSpec = table.bucketSpec,
////      options = table.storage.properties ++ pathOption,
////      catalogTable = if (tableExists) Some(table) else None)
////
////    try {
////      dataSource.writeAndRead(mode, query, outputColumnNames)
////    } catch {
////      case ex: AnalysisException =>
////        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
////        throw ex
////    }
////  }
////}
//case class CustomDataSourceAsSelectCommand(
//                                          catalog: TableCatalog,
//                                       table: CatalogTable,
//                                       mode: SaveMode,
//                                       query: LogicalPlan,
//                                       outputColumnNames: Seq[String])
//  extends LeafRunnableCommand {
//  //assert(query.resolved)
//  override def innerChildren: Seq[LogicalPlan] = query :: Nil
//
//  override def run(sparkSession: SparkSession): Seq[Row] = {
//   // assert(table.tableType != CatalogTableType.VIEW)
//   // assert(table.provider.isDefined)
//
////    val sessionState = sparkSession.sessionState
////    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
////    val tableIdentWithDB = table.identifier.copy(database = Some(db))
////    val tableName = tableIdentWithDB.unquotedString
//
//    saveDataIntoTable(
//      sparkSession, table, table.storage.locationUri, SaveMode.Append, tableExists = true)
////    if (catalog.tableExists(table.identifier)) {
////      saveDataIntoTable(
////        sparkSession, table, table.storage.locationUri, SaveMode.Append, tableExists = true)
////    } else {
////      table.storage.locationUri.foreach { p =>
////        DataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
////      }
//      assert(table.schema.isEmpty)
////      sparkSession.sessionState.catalog.validateTableLocation(table)
////      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
////        Some(sessionState.catalog.defaultTablePath(table.identifier))
////      } else {
////        table.storage.locationUri
////      }
////      val result = saveDataIntoTable(
////        sparkSession, table, tableLocation, SaveMode.Overwrite, tableExists = false)
////      val tableSchema = CharVarcharUtils.getRawSchema(
////        removeInternalMetadata(result.schema), sessionState.conf)
////      val newTable = table.copy(
////        storage = table.storage.copy(locationUri = tableLocation),
////        // We will use the schema of resolved.relation as the schema of the table (instead of
////        // the schema of df). It is important since the nullability may be changed by the relation
////        // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
////        schema = tableSchema)
//      // Table location is already validated. No need to check it again during table creation.
//   //   sessionState.catalog.createTable(newTable, ignoreIfExists = true, validateLocation = false)
//
////      result match {
////        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
////          sparkSession.sqlContext.conf.manageFilesourcePartitions =>
////          // Need to recover partitions into the metastore so our saved data is visible.
////          sessionState.executePlan(RepairTableCommand(
////            table.identifier,
////            enableAddPartitions = true,
////            enableDropPartitions = false), CommandExecutionMode.SKIP).toRdd
////        case _ =>
////      }
//  //  }
//
// //   CommandUtils.updateTableStats(sparkSession, table)
//
//    Seq.empty[Row]
//  }
//
//  private def saveDataIntoTable(
//                                 session: SparkSession,
//                                 table: CatalogTable,
//                                 tableLocation: Option[URI],
//                                 mode: SaveMode,
//                                 tableExists: Boolean): BaseRelation = {
//    // Create the relation based on the input logical plan: `query`.
//    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
//    val dataSource = DataSource(
//      session,
//      className = table.provider.get,
//      partitionColumns = table.partitionColumnNames,
//      bucketSpec = table.bucketSpec,
//      options = table.storage.properties ++ pathOption,
//      catalogTable = if (tableExists) Some(table) else None)
//
//    try {
//      InsertIntoHadoopFsRelationCommand(
//        outputPath = new Path(table.storage.locationUri.get.toString),
//        staticPartitions = Map.empty,
//        ifPartitionNotExists = false,
//        partitionColumns = Seq.empty[Attribute],
//        bucketSpec = None,
//        fileFormat = getFileFormat(table.provider.getOrElse("csv")),
//        Map.empty,
//        query = query,
//        SaveMode.Append,
//        None,
//        None,
//        query.output.map(_.name)
//      )
//    } catch {
//      case ex: AnalysisException =>
//        logError(s"Failed to write to table ${table.identifier.unquotedString}", ex)
//        throw ex
//    }
//  }
//
//  def getFileFormat(formatName:String):FileFormat ={
//    formatName.toLowerCase match {
//      case "csv" => new CSVFileFormat
//      case "orc" => new OrcFileFormat
//      case "parquet" => new ParquetFileFormat
//      case "orc" => new OrcFileFormat
//      case "avro" => new AvroFileFormat
//      case _  => new CSVFileFormat
//    }
//  }
//}

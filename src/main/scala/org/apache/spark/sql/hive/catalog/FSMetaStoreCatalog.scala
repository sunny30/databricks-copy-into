package org.apache.spark.sql.hive.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogTable, CatalogTablePartition, CatalogTableType, ExternalCatalog, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.QueryExecution

import java.io.IOException
import scala.collection.mutable

class FSMetaStoreCatalog(

                        catalogName: String,
                        sparkConf: SparkConf,
                        hadoopConfig: Configuration = new Configuration,
                        sparkSession: SparkSession = SparkSession.active
                        ) extends ExternalCatalog  {

  val warehousePath = sparkConf.get("spark.sql.warehouse.dir")
  val catalogPath = new Path(warehousePath, catalogName+".cat")
  val fs = catalogPath.getFileSystem(hadoopConfig)

  private def createCatalogDirectory(): Unit = {
    if(!fs.exists(catalogPath)){
      fs.mkdirs(catalogPath)
    }
  }

  private class TableDesc(var table: CatalogTable) {
    var partitions = new mutable.HashMap[TablePartitionSpec, CatalogTablePartition]
  }

  private class DatabaseDesc(var db: CatalogDatabase) {
    val tables = new mutable.HashMap[String, TableDesc]
    val functions = new mutable.HashMap[String, CatalogFunction]
  }

  private val catalog = new scala.collection.mutable.HashMap[String, DatabaseDesc]


  override def databaseExists(db: String): Boolean = {
    val dbPath = new Path(catalogPath, db+".db")
    fs.exists(dbPath)
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val dbName = dbDefinition.name
    if(databaseExists(dbName)){
      if(!ignoreIfExists){
        throw  new IllegalArgumentException("Database already exists")
      }
    }else{
      val dbPath = new Path(catalogPath, dbName+".db")
      fs.mkdirs(dbPath)
      val newDb = dbDefinition.copy(
        properties = dbDefinition.properties,
        locationUri = dbPath.toUri
      )
      catalog.put(dbName, new DatabaseDesc(newDb))
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val dbPath = new Path(catalogPath, db+".db")
    if(databaseExists(db)){
      fs.delete(dbPath, true)
    }else{
      if(!ignoreIfNotExists){
        throw  new IllegalArgumentException("Database already exists")
      }
    }
  }


  override def renameTable(db: String, oldName: String, newName: String): Unit = {


  }


  override def tableExists(db: String, table: String): Boolean = {
    val dbPath = new Path(catalogPath, db+".db")
    val tablePath = new Path(dbPath, table)
    fs.exists(tablePath)
  }

  override def getDatabase(db: String): CatalogDatabase = {
    if(databaseExists(db)){
      catalog(db).db
    }else{
      throw new IllegalArgumentException("database does not exist")
    }
  }

  override def listDatabases(pattern: String): Seq[String] = {
    fs.listStatus(catalogPath).filter(fs => fs.isDirectory).
      filter(dir => dir.getPath.getName.endsWith(".db")).
      map(db => db.getPath.getName)
  }

  override def listDatabases(): Seq[String] = {
    fs.listStatus(catalogPath).filter(fs => fs.isDirectory).
      map(db => db.getPath.getName)
  }


  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = {
    throw QueryExecutionErrors.methodNotImplementedError("loadTable")
  }

  override def getTable(db: String, table: String): CatalogTable = {
    try {
      catalog(db).tables(table).table
    }catch {
      case e:Exception => null
    }
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    tables.flatMap(catalog(db).tables.get).map(_.table)
  }

  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    throw QueryExecutionErrors.methodNotImplementedError("list partition")
  }


  private def toCatalogPartitionSpec = ExternalCatalogUtils.convertNullPartitionValues(_)

  private def toCatalogPartitionSpecs(specs: Seq[TablePartitionSpec]): Seq[TablePartitionSpec] = {
    specs.map(toCatalogPartitionSpec)
  }

  private def toCatalogPartitionSpec(
                                      parts: Seq[CatalogTablePartition]): Seq[CatalogTablePartition] = {
    parts.map(part => part.copy(spec = toCatalogPartitionSpec(part.spec)))
  }


  override def getPartition(
                               db: String,
                               table: String,
                               partSpec: TablePartitionSpec): CatalogTablePartition = synchronized{

    val spec = toCatalogPartitionSpec(partSpec)
    //requirePartitionsExist(db, table, Seq(spec))
    catalog(db).tables(table).partitions(spec)

  }


  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    throw QueryExecutionErrors.methodNotImplementedError("list partition")
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    throw QueryExecutionErrors.methodNotImplementedError("list Function")
  }

  override def listViews(db: String, pattern: String): Seq[String] = {
    throw QueryExecutionErrors.methodNotImplementedError("list views")
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {

    val db = tableDefinition.identifier.database.get
    val table = tableDefinition.identifier.table
    if (tableExists(db, table)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = db, table = table)
      }
    } else {
      val needDefaultTableLocation =
        tableDefinition.tableType == CatalogTableType.MANAGED

      val tableWithLocation = if (needDefaultTableLocation) {
        val defaultTableLocation = new Path(new Path(catalog(db).db.locationUri), table)
        try {
          // val fs = defaultTableLocation.getFileSystem(hadoopConfig)
          fs.mkdirs(defaultTableLocation)
        } catch {
          case e: IOException =>
            throw QueryExecutionErrors.unableToCreateTableAsFailedToCreateDirectoryError(
              table, defaultTableLocation, e)
        }

        tableDefinition.withNewStorage(locationUri = Some(defaultTableLocation.toUri))

      } else {
          tableDefinition
        }
      val tableProp = tableWithLocation.properties.filter(_._1 != "comment")
      catalog(db).tables.put(table, new TableDesc(tableWithLocation.copy(identifier=TableIdentifier(tableWithLocation.identifier.table, database = Some(tableDefinition.database),catalog = Some(catalogName)),properties = tableProp)))
    }
  }

  override def functionExists(db: String, funcName: String): Boolean = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("Function exists")
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("get Function")
  }

  override def renameFunction(
                               db: String,
                               oldName: String,
                               newName: String): Unit = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("rename function")
  }

  override def dropFunction(db: String, funcName: String): Unit = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("drop function")
  }

  override def alterFunction(db: String, func: CatalogFunction): Unit = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("alter function")
  }

  override def createFunction(db: String, func: CatalogFunction): Unit = synchronized {
    throw QueryExecutionErrors.methodNotImplementedError("create function")
  }



  override def listTables(db: String): Seq[String] = synchronized {
    catalog(db).tables.keySet.toSeq.sorted
  }

  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = {
    throw QueryExecutionErrors.methodNotImplementedError("rename partition")
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    throw QueryExecutionErrors.methodNotImplementedError("rename partition")
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    catalog(db).tables.keySet.toSeq.filter(n => n.matches(pattern)).sorted
  }

  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = {
    throw QueryExecutionErrors.methodNotImplementedError("alter partition")
  }

  override def setCurrentDatabase(db: String): Unit = ???

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    println("empty drop impl")
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = ???

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = ???

  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = ???

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = ???

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = ???

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = ???

  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = ???

  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = ???
}

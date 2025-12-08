package org.apache.spark.sql.iceberg

import org.apache.iceberg.hadoop.UnitySparkCatalog
import org.apache.hadoop.fs.Path
import org.apache.iceberg
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, ExternalCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, Table, TableChange, TableSchemaChangeCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.iceberg.catalog.Procedure
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}

import java.util
import java.util.regex.Pattern
class UnityIcebergCatalog(plugin: ExternalCatalog, catalogName: String,options: CaseInsensitiveStringMap) extends DeltaLogging with Catalog{

  lazy val icebergCatalog = {
    val ct = new UnitySparkCatalog()
    ct.initialize(catalogName, catalogOptions(catalogName, SQLConf.get))
    ct
  }

  def createIcebergTable(
                          ident: Identifier,
                          schema: StructType,
                          partitions: Array[Transform],
                          allTableProperties: java.util.Map[String, String],
                          catalogTable: CatalogTable
                        ): Table ={
    val ct = new SparkCatalog()

    val icebergCatalog = new UnitySparkCatalog()
    val initializedCatalog = icebergCatalog.initialize(catalogName, catalogOptions(catalogName, SQLConf.get))
    val icebergTable = icebergCatalog.createTable(ident, schema,partitions,allTableProperties)

    plugin.createTable(catalogTable, true)
    icebergTable

  }


  private def catalogOptions(name: String, conf: SQLConf) = {
    conf.setConfString("spark.sql.catalog.cat.type", "hadoop")
    conf.setConfString("spark.sql.catalog.cat.warehouse", getCatlogPath)

    val prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)")
    val options = new util.HashMap[String, String]
    options.put("warehouse", getCatlogPath)
    options.put("type", "hadoop")
    conf.getAllConfs.foreach {
      case (key, value) =>
        val matcher = prefix.matcher(key)
        if (matcher.matches && matcher.groupCount > 0) {
          options.put(matcher.group(1), value)
          println("kv"+matcher.group(1)+":"+value)
        }
    }
    new CaseInsensitiveStringMap(options)
  }


  def getCatlogPath:String = {
    val warehousePath = SparkSession.active.sharedState.conf.get("spark.sql.warehouse.dir")
    val catalogPath = new Path(warehousePath, catalogName + ".cat")
    println("getCatalogPath is"+ catalogPath.toString)
    catalogPath.toString
  }


  def loadTable(ident: Identifier): Table = {
    val icebergTable = icebergCatalog.loadTable(ident)
    icebergTable
  }

  def loadTable(ident: Identifier, timestamp: Long): Table={
    val icebergTable = icebergCatalog.loadTable(ident,timestamp)
    icebergTable
  }

   def loadTable(ident: Identifier, version: String): Table={
    val icebergTable = icebergCatalog.loadTable(ident, version)
    icebergTable
  }

  def stageReplace(
                    ident: Identifier,
                    schema: StructType,
                    partitions: Array[Transform],
                    properties: util.Map[String, String]): StagedTable = {
    icebergCatalog.stageReplace(ident, schema, partitions, properties)
  }

  def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable ={
    icebergCatalog.stageCreate(ident,schema,partitions,properties)
  }

  def stageCrateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): StagedTable ={
    icebergCatalog.stageCreateOrReplace(ident,schema,partitions,properties)
  }

  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit ={
    icebergCatalog.renameTable(oldIdent, newIdent)
    val oldTableName = oldIdent.asTableIdentifier.table
    val newTableName = newIdent.asTableIdentifier.table
    val dbName = newIdent.asTableIdentifier.database.getOrElse("default")
    plugin.renameTable(dbName, oldTableName, newTableName)
  }

  def dropTable(ident: Identifier): Boolean = {
    icebergCatalog.dropTable(ident)
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    plugin.dropTable(dbName, tableName, true, false)
    true
  }

  def alterTable(ident: Identifier, changes: TableChange*): Table={
    icebergCatalog.alterTable(ident, changes:_*)
  }

  def loadProcedure(identifier: Identifier): Procedure = {
    icebergCatalog.loadProcedure(identifier)
  }

  override def listTables(namespace: Namespace): util.List[TableIdentifier] = ???

  override def dropTable(tableIdentifier: TableIdentifier, b: Boolean): Boolean = ???

  override def renameTable(tableIdentifier: TableIdentifier, tableIdentifier1: TableIdentifier): Unit = ???

  override def loadTable(tableIdentifier: TableIdentifier): iceberg.Table = ???

  override def registerTable(tableIdentifier: TableIdentifier, metadataFileLocation:String): org.apache.iceberg.Table={
    val databaseName = tableIdentifier.namespace().level(0) ;
    val tableName = tableIdentifier.name()
    if(!plugin.tableExists(databaseName, tableName)) {
      val ct = getMetastoreTable(databaseName,tableName)
      SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableSchemaChangeCatalog].registerTableInMetastore(ct)
      icebergCatalog.registerTable(tableIdentifier,metadataFileLocation)
    }else{
      throw new AnalysisException("Table already exists")
    }
  }

  private def getMetastoreTable(databaseName:String, tableName:String):CatalogTable={

    val storage: CatalogStorageFormat = CatalogStorageFormat(None, None, None, None, false, Map.empty[String,String])
    val ct = CatalogTable(
      identifier = org.apache.spark.sql.catalyst.TableIdentifier(tableName, Some(databaseName), Some(catalogName)),
      CatalogTableType.MANAGED,
      storage =  storage,
      schema = StructType.apply(Seq.empty[StructField]),
      provider = Some("iceberg"),
    )
    //SparkSession.active.sessionState.catalogManager.catalog(catalogName).asInstanceOf[TableSchemaChangeCatalog].registerTableInMetastore(ct)
    ct
  }
}

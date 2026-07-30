package org.apache.spark.sql.hive.catalog

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class LiveCatalog[T <: TableCatalog with SupportsNamespaces] extends CatalogExtension
  with SupportsNamespaces with DeltaLogging{

  private var catalogName: String = null

  private var delegatedCatalog: CatalogPlugin = null

  var options: CaseInsensitiveStringMap = null
  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = {
    log.info("Inside set Delegated of Catalog Extension")
    // Check if the delegating catalog has Table and SupportsNamespace properties
    if (delegate.isInstanceOf[TableCatalog] && delegate.isInstanceOf[SupportsNamespaces]) {
      this.delegatedCatalog = delegate
      // Set delegated catalog in any other provider that we can integrate with
    } else throw new IllegalArgumentException("Invalid session catalog: " + delegate)
  }

  override def listNamespaces(): Array[Array[String]] = {
    val df = SparkSession.active.read.format("csv").option("header", "true").load("/Users/sharadsingh/Documents/more_schemas/schema.csv")
    val resultMap: Map[String, Array[String]] = dataFrameToColumnMap(df)
    resultMap.flatMap(r => r._2.map(Array(_))).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {

    namespace match {
      case Array() =>
        listNamespaces()
      case Array(f,db) =>
        listNamespaces()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {

    namespace match {
      case Array(f,db) =>
        val odb = CatalogDatabase(
          name = db,
          description = "reserve-database for systems",
          locationUri = CatalogUtils.stringToURI("NA"),
          properties = Map.empty[String, String]
        )
        val augmentedProperties = odb.properties ++ Map("db_location" -> "live_db")
        augmentedProperties.asJava
    }
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    throw new AnalysisException("Create database not supported")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new AnalysisException("Alter database not supported")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    throw new AnalysisException("Drop database not supported")
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    throw new AnalysisException("List function in live catalog not supported")
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    throw new AnalysisException("Loading function in live catalog not supported")

  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {

    namespace match {
      case Array(f,db) =>
        val ident = Identifier.of(Array(db), "live_tbl")
        Array(ident)
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val ct = getLiveTableMetadata(ident)
    V2Table(ct)
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = ???

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {

  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    log.info("Inside Catalog Plugin Initialize")
    this.catalogName = name
    this.options = options

  }

  private def getLiveTableMetadata(ident:Identifier):CatalogTable={
    val dbName = ident.namespace()
    val tableName = ident.name()
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", StringType, nullable = true)
    ))
    dbName match {
      case Array(f,db) =>
        CatalogTable(
          identifier = TableIdentifier(ident.name, Some(db), Some(catalogName)),
          CatalogTableType.EXTERNAL,
          new CatalogStorageFormat(None, None, None,
            None, false, Map.empty[String, String]
          ),
          schema,
          provider = Some("custom")
        )
      case Array(db) =>
        CatalogTable(
          identifier = TableIdentifier(ident.name, Some(db), Some(catalogName)),
          CatalogTableType.EXTERNAL,
          new CatalogStorageFormat(None, None, None,
            None, false, Map.empty[String, String]
          ),
          schema,
          provider = Some("custom")
        )

      case _ => throw new AnalysisException("table does not exist")
    }
  }

  override def name(): String = catalogName


  def dataFrameToColumnMap(df: DataFrame): Map[String, Array[String]] = {
    val columnNames: Array[String] = df.columns
    val rows: Array[Row] = df.collect()

    columnNames.map { colName =>
      val values: Array[String] = rows.map { row =>
        val v = row.getAs[Any](colName)
        if (v == null) null else v.toString
      }
      colName -> values
    }.toMap
  }
}

package org.apache.spark.sql.hive.catalog

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.delta.implicits.stringEncoder

import scala.util.matching.Regex
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConverters._

class LiveCatalog[T <: TableCatalog with SupportsNamespaces] extends CatalogExtension
  with SupportsNamespaces with DeltaLogging {

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
      case Array(f, db) =>
        listNamespaces()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }


  override def namespaceExists(namespace: Array[String]): Boolean = {
    namespace match {
      case Array(f, db) =>
        listNamespaces().map(p => p).exists(_.head.equalsIgnoreCase(db))
      case Array(db) =>
        listNamespaces().map(p => p).exists(_.head.equalsIgnoreCase(db))
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {

    namespace match {
      case Array(f, db) if namespaceExists(namespace) =>
        val df = SparkSession.active.read.format("csv").option("header", "true").load("/Users/sharadsingh/Documents/more_schemas/schema_properties.csv")
        val resultMap: Map[String, Array[String]] = dataFrameToColumnMap(df)
        val res = resultMap.map(r => (r._1, r._2.head))
        res.asJava

      case Array(db) if namespaceExists(namespace) =>
        val df = SparkSession.active.read.format("csv").option("header", "true").load("/Users/sharadsingh/Documents/more_schemas/schema_properties.csv")
        val resultMap: Map[String, Array[String]] = dataFrameToColumnMap(df)
        val res = resultMap.map(r => (r._1, r._2.head))
        res.asJava


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

  override def tableExists(ident: Identifier): Boolean = {
    listTables(ident.namespace()).
      map(i => i.name()).
      exists(t => t.equalsIgnoreCase(ident.name()))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {

    namespace match {
      case Array(f, db) =>
        val df = SparkSession.active.read.format("csv").option("header", "true").load("/Users/sharadsingh/Documents/more_schemas/name_type.csv")
        val resultMap: Map[String, Array[String]] = dataFrameToColumnMap(df)
        resultMap.get("name") match {
          case Some(v) =>
            v.map(tbl => Identifier.of(Array(db), tbl))

          case None => Array.empty[Identifier]
        }

      case Array(db) =>
        val df = SparkSession.active.read.format("csv").option("header", "true").load("/Users/sharadsingh/Documents/more_schemas/name_type.csv")
        val resultMap: Map[String, Array[String]] = dataFrameToColumnMap(df)
        resultMap.get("name") match {
          case Some(v) =>
            v.map(tbl => Identifier.of(Array(db), tbl))

          case None => Array.empty[Identifier]
        }

      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val df = SparkSession.active.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load("/Users/sharadsingh/Documents/more_schemas/table_schema_dataset_event.csv")
    val ct = getCatalogTable(df, ident)
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

  private def getLiveTableMetadata(ident: Identifier): CatalogTable = {
    val dbName = ident.namespace()
    val tableName = ident.name()
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("age", StringType, nullable = true)
    ))
    dbName match {
      case Array(f, db) =>
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

  def getCatalogTable(df: DataFrame, ident: Identifier): CatalogTable = {
    val resultMap = dataFrameToColumnMap(df)
    resultMap.get("dataset_event") match {
      case Some(v) =>
        //val df = SparkSession.active.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv("path/to/file.csv")
        val rawJson = df.select("dataset_event").as[String].collect().head
        val root = DatasetEventParser.parseFromCsvParsedField(rawJson)
        val scheaFieldNde = root.path("dataset").path("facets").path("schema").path("fields")
        val schema = JsonFieldsToStructType.toStructType(scheaFieldNde)

        val dbName = ident.namespace().head
        val tableName = ident.name()
        CatalogTable(
          identifier = TableIdentifier(tableName, Some(dbName), Some(catalogName)),
          CatalogTableType.EXTERNAL,
          new CatalogStorageFormat(None, None, None,
            None, false, Map.empty[String, String]
          ),
          schema,
          provider = Some("custom"),
          properties = Map.empty[String, String]
        )


      case None => throw new AnalysisException("Metadata missing from connector")
    }
  }
}




  object DatasetEventFixer {

    // Stage 1: undo CSV-style quote doubling ("" -> ")
    // Only needed if you're handed the raw literal text, NOT if a CSV
    // parser (Spark, OpenCSV, commons-csv) already read the field for you.
    def unescapeCsvQuotes(raw: String): String = {
      // strip one layer of the outer wrapping quote if present, then unescape ""
      val trimmed =
        if (raw.startsWith("\"") && raw.endsWith("\"")) raw.substring(1, raw.length - 1)
        else raw
      trimmed.replace("\"\"", "\"")
    }

    // Stage 2: fix the XSA("uuid"."table") embedded-quote issue
    private val xsaFieldPattern: Regex =
      """"(name|displayName)":"(XSA\(.*?\))"""".r

    def sanitize(raw: String): String = {
      xsaFieldPattern.replaceAllIn(raw, m => {
        val fieldName = m.group(1)
        val xsaValue = m.group(2).replace("\"", "\\\"")
        Regex.quoteReplacement(s""""$fieldName":"$xsaValue"""")
      })
    }

    // Full pipeline: raw CSV-literal text -> parseable JSON string
    def fixCsvEscapedJson(rawFromCsvLiteral: String): String = {
      val csvUnescaped = unescapeCsvQuotes(rawFromCsvLiteral)
      sanitize(csvUnescaped)
    }
  }

  object DatasetEventParser {
    private val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    // Use this if the value already came through a real CSV parser
    // (Spark df.select("dataset_event"), OpenCSV, etc.) — only the
    // XSA quote issue remains.
    def parseFromCsvParsedField(value: String): JsonNode =
      mapper.readTree(DatasetEventFixer.sanitize(value))

    // Use this if you have the raw literal CSV text with visible "" escaping,
    // e.g. copy-pasted straight from the file without going through a CSV reader.
    def parseFromRawCsvLiteral(rawLiteral: String): JsonNode =
      mapper.readTree(DatasetEventFixer.fixCsvEscapedJson(rawLiteral))
  }

  object JsonFieldsToStructType {

    private val decimalPattern: Regex = """decimal\((\d+),(\d+)\)""".r

    // Maps the type string coming from the schema facet into a Spark DataType
    def mapType(typeStr: String): DataType = typeStr.trim.toLowerCase match {
      case "string" => StringType
      case "timestamp" => TimestampType
      case "date" => DateType
      case "boolean" | "bool" => BooleanType
      case "int" | "integer" => IntegerType
      case "long" | "bigint" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "binary" => BinaryType
      case decimalPattern(prec, scale) => DecimalType(prec.toInt, scale.toInt)
      case other =>
        // Fallback: unknown/unsupported type -> treat as String rather than throw
        StringType
    }

    /**
     * @param fieldsNode the JsonNode at ...facets.schema.fields (must be an ARRAY node)
     * @param nullable   whether generated fields should be nullable (default true)
     */
    def toStructType(fieldsNode: JsonNode, nullable: Boolean = true): StructType = {
      require(fieldsNode.isArray, "Expected 'fields' to be a JSON array")

      val structFields: Array[StructField] = fieldsNode.elements().asScala.map { fieldNode =>
        val name = fieldNode.path("name").asText()
        val typeStr = fieldNode.path("type").asText()
        StructField(name, mapType(typeStr), nullable)
      }.toArray

      StructType(structFields)
    }
  }




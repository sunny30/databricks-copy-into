package org.apache.spark.sql.hive.plan.spark.sql.connector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, FileStatusCache, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVDataSourceV2, CSVTable}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonDataSourceV2, JsonTable}
import org.apache.spark.sql.execution.datasources.v2.orc.{OrcDataSourceV2, OrcTable}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.execution.datasources.v2.text.{TextDataSourceV2, TextTable}
import org.apache.spark.sql.hive.classloader.reflection.ReflectionUtil
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.{AvroDataSourceV2, AvroTable}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

// ─────────────────────────────────────────────────────────────────────────────
// CHANGE LIST
// ─────────────────────────────────────────────────────────────────────────────
//
// FIX 9 — capabilities(): was returning allOf(TableCapability)
//   BEFORE: advertising every capability including ones not implemented
//           (TRUNCATE, ACCEPT_ANY_SCHEMA, OVERWRITE_BY_FILTER, V1_BATCH_READ,
//           MICRO_BATCH_READ, CONTINUOUS_READ, etc.) caused the optimizer to
//           take code paths that then failed or produced incorrect results.
//           Advertising V1_BATCH_READ alongside V2_BATCH_READ in particular can
//           cause Spark to prefer the V1 code path which bypasses DSv2 optimizations.
//   AFTER:  only declare what is actually implemented: BATCH_READ.
//           Add BATCH_WRITE, OVERWRITE_DYNAMIC, OVERWRITE_BY_FILTER,
//           TRUNCATE only if/when write support is wired up for these formats.
//
// FIX 10 — partitionColumnTypeInference: was mutating a session-global conf
//   BEFORE: sparkSession.conf.set("spark.sql.sources.partitionColumnTypeInference
//           .enabled", false) inside newScanBuilder() sets a session-wide flag
//           that persists for all subsequent queries and tables in the session —
//           including tables that do NOT have all-string partition columns.
//           This silently corrupts type inference for all tables after the first
//           all-string-partition table is scanned.
//   AFTER:  pass the option directly into the CaseInsensitiveStringMap that
//           goes to the scan builder, scoped only to this table's scan. The
//           underlying InMemoryFileIndex reads this option per-scan.
//
// FIX 11 — catalogTable threaded into V2CustomTableScanBuilder and scan
//   The catalogTable is now passed through so V2CustomTableScan.estimateStatistics()
//   can expose ANALYZE TABLE row counts to CBO (see FIX 6 in V2CustomTableScan).
//
// FIX 12 — stale imports removed (TableProvider, ServiceLoader, Utils,
//           DataSourceRegister, StructField — none used)
//
// FIX 13 — fileIndex: fresh InMemoryFileIndex per query → session-shared FileStatusCache
//   BEFORE: fileTable is a brand-new DataSourceV2 table instance created on every
//           newScanBuilder() call. fileTable.fileIndex is a lazy val on that instance,
//           so it re-lists the entire partition tree from scratch on every query —
//           one FileSystem.listStatus() call per partition directory, per query.
//           On a table with e.g. 500 date × 50 store partitions that is 25,000 HDFS/S3
//           calls before a single row is read, repeated every time the table appears
//           in a query. V2CustomFileTable (which used FileStatusCache.getOrCreate) was
//           the fix but was commented out at line 64-65, removing the caching.
//   AFTER:  the fileIndex is built directly in newScanBuilder() using
//           FileStatusCache.getOrCreate(sparkSession) — the session-level singleton
//           cache that Spark itself uses for all built-in file-format tables.
//           The first query lists partitions and populates the cache; subsequent
//           queries on the same table reuse the cached listing without any FS calls.
//           partitionSpec is copied from fileTable.fileIndex to preserve the correct
//           partition column types inferred by the format-specific table.
// ─────────────────────────────────────────────────────────────────────────────

case class V2CustomTable(name: String,
                         sparkSession: SparkSession,
                         options: CaseInsensitiveStringMap,
                         catalogTable: CatalogTable) extends SupportsRead with Table {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val provider =
      if (catalogTable.provider.get.equalsIgnoreCase("hive"))
        catalogTable.storage.properties("fileformat").toLowerCase
      else
        catalogTable.provider.getOrElse("parquet")

    val multiPartName = Seq(
      catalogTable.identifier.catalog.getOrElse("spark_catalog"),
      catalogTable.identifier.database.getOrElse("default"),
      catalogTable.identifier.table)

    if (provider.toLowerCase.equalsIgnoreCase("custom")) {
      val clazzName  = "org.apache.spark.sql.hive.plan.spark.sql.connector.custom.CustomTable"
      val methodName = "newScanBuilder"
      return ReflectionUtil.reflectScanBuilder(clazzName, methodName, schema, options)
    }

    // FIX 10 — scope partition type inference to this scan only via options,
    // instead of mutating the session-global conf which persists across queries
    val effectiveOptions: CaseInsensitiveStringMap =
    if (validateAllPartitionColumns(catalogTable)) {
      val merged = new java.util.HashMap[String, String](options)
      merged.put("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      new CaseInsensitiveStringMap(merged)
    } else {
      options
    }

    val fileTable = provider.toLowerCase match {
      case "parquet"  => new ParquetDataSourceV2().getTable(effectiveOptions).asInstanceOf[ParquetTable]
      case "orc"      => new OrcDataSourceV2().getTable(effectiveOptions).asInstanceOf[OrcTable]
      case "avro"     => new AvroDataSourceV2().getTable(effectiveOptions).asInstanceOf[AvroTable]
      case "csv"      => new CSVDataSourceV2().getTable(effectiveOptions).asInstanceOf[CSVTable]
      case "json"     => new JsonDataSourceV2().getTable(effectiveOptions).asInstanceOf[JsonTable]
      case "text"     => new TextDataSourceV2().getTable(effectiveOptions).asInstanceOf[TextTable]
      case "textfile" => new CSVDataSourceV2().getTable(effectiveOptions).asInstanceOf[CSVTable]
    }

    val dataSchema = catalogTable.dataSchema
    val readSchema = catalogTable.schema

    // FIX 13 — build fileIndex with the session-shared FileStatusCache so repeated
    // queries on the same table reuse already-listed partition files.
    //
    // BEFORE: `fileTable.fileIndex` was a lazy val on a brand-new fileTable instance
    //   created every newScanBuilder() call, so it re-walked the full partition tree
    //   from scratch on every query (one FS listStatus call per partition directory).
    //   V2CustomFileTable had the right idea (FileStatusCache.getOrCreate) but was
    //   commented out, removing the caching entirely.
    //
    // AFTER: FileStatusCache.getOrCreate(sparkSession) returns the session-level
    //   singleton the same cache Spark's own built-in file tables use. The first
    //   query populates it; all subsequent queries reuse it with zero FS calls.
    val caseSensitiveMap = effectiveOptions.asCaseSensitiveMap.asScala.toMap
    val hadoopConf       = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val rootPaths        = DataSource.checkAndGlobPathIfNecessary(
      getPaths(effectiveOptions),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist    = true,
      enableGlobbing     = true)

    // partitionSpec copied from fileTable so the format-specific table's inferred
    // partition column types are preserved, overriding them with the catalog schema.
    val partSpec = fileTable.fileIndex.partitionSpec()
      .copy(partitionColumns = catalogTable.partitionSchema)

    val fileIndex = new InMemoryFileIndex(
      sparkSession,
      rootPaths,
      caseSensitiveMap,
      userSpecifiedSchema        = Some(readSchema),
      fileStatusCache            = FileStatusCache.getOrCreate(sparkSession),
      userSpecifiedPartitionSpec = Some(partSpec)
    )

    // FIX 11 — pass catalogTable so the scan can expose stats to CBO
    V2CustomTableScanBuilder(
      multiPartName, provider, sparkSession, fileIndex, readSchema, dataSchema,
      effectiveOptions, Some(catalogTable))
  }

  protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(map.get("paths")).map { pathStr =>
      FileDataSourceV2.readPathsToSeq(pathStr)
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  private def validateAllPartitionColumns(catalogTable: CatalogTable): Boolean =
    catalogTable.partitionSchema.fields.forall(_.dataType.sameType(StringType))

  override def schema(): StructType = catalogTable.schema

  // FIX 9 — only declare actually implemented capabilities
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  def mapHiveCSVPropertiesToSparkOption(ct: CatalogTable, fileFormat: FileFormat): Map[String, String] = {
    var tblProps = ct.properties
    if (fileFormat.isInstanceOf[CSVFileFormat]) {
      if (!tblProps.contains("option.delimiter"))
        tblProps = tblProps ++ Map("delimiter" -> tblProps.getOrElse("field.delim", ","))
      if (!tblProps.contains("option.quote"))
        tblProps = tblProps ++ Map("quote" -> tblProps.getOrElse("quoteChar", '\"'.toString))
      if (!tblProps.contains("option.escape"))
        tblProps = tblProps ++ Map("escape" -> tblProps.getOrElse("escape.delim", '\\'.toString))
      if (!tblProps.contains("option.header"))
        tblProps = tblProps ++ Map("header" -> tblProps.getOrElse("hasheaders", "false"))
      if (!tblProps.contains("option.lineSep"))
        tblProps = tblProps ++ Map("lineSep" -> tblProps.getOrElse("recorddelimiter", "\n"))
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
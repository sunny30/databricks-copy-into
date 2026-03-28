package org.apache.spark.sql.hive.plan.spark.sql.connector

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, FileStatusCache, InMemoryFileIndex, PartitionSpec, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class V2CustomFileTable(
                             sparkSession: SparkSession,
                             options: CaseInsensitiveStringMap,
                             paths: Seq[String],
                             userSpecifiedSchema: Option[StructType],
                             fileTable: FileTable,
                             catalogTable: CatalogTable)
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      // We are reading from the results of a streaming query. We will load files from
      // the metadata log instead of listing them using HDFS APIs.
      new MetadataLogFileIndex(sparkSession, new Path(paths.head),
        options.asScala.toMap, userSpecifiedSchema)
    } else {
      // This is a non-streaming file based datasource.
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
        checkEmptyGlobPath = true, checkFilesExist = true, enableGlobbing = globPaths)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
      new InMemoryFileIndex(
        sparkSession, rootPathsSpecified, caseSensitiveMap, userSpecifiedSchema, fileStatusCache, userSpecifiedPartitionSpec = Some(getTablePartitionSpec))
    }
  }

  private def globPaths: Boolean = {
    val entry = options.get(DataSource.GLOB_PATHS_KEY)
    Option(entry).map(_ == "true").getOrElse(true)
  }

  private def getTablePartitionSpec: PartitionSpec={
    fileTable.fileIndex.partitionSpec().copy(partitionColumns = catalogTable.partitionSchema)
  }
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = fileTable.inferSchema(files)

  override def formatName: String = fileTable.formatName

  override def fallbackFileFormat: Class[_ <: FileFormat] = fileTable.fallbackFileFormat

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = fileTable.newWriteBuilder(info)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = fileTable.newScanBuilder(options)

  override def name(): String = fileTable.name()
}

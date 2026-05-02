package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi


import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.io.Serializable
import java.util.OptionalLong
import scala.collection.JavaConverters._

// ─── InputPartition types ─────────────────────────────────────────────────────

/**
 * COW: one InputPartition per base Parquet file.
 */
case class HudiCOWPartition(
                             path: String,
                             fileSize: Long,
                             partitionValues: InternalRow
                           ) extends InputPartition with Serializable

/**
 * MOR: one InputPartition per file group — base file (optional) + all log files.
 * HudiMORPartitionReader merges them via HoodieMergedLogRecordScanner.
 * basePath = None for insert-only file groups that have no base Parquet file yet.
 */
case class HudiMORPartition(
                             basePath: Option[String],
                             logPaths: Array[String],
                             fileGroupId: String,
                             partitionPath: String,
                             latestInstantTime: String,
                             fileSize: Long
                           ) extends InputPartition with Serializable

// ─── COW Scan ─────────────────────────────────────────────────────────────────

/**
 * COW Scan — lists latest base Parquet file slices via HoodieFileIndex.
 *
 * Spark 3.5 interfaces:
 *   Scan.readSchema()           — projected schema
 *   Batch.planInputPartitions() — one HudiCOWPartition per file
 *   SupportsReportStatistics    — total byte size for CBO join ordering
 *
 * HoodieFileIndex.listFiles() returns Seq[PartitionDirectory].
 * PartitionDirectory.files is Array[FileStatus] (standard hadoop FileStatus,
 * unchanged in Hudi 1.0.1 at the Spark integration boundary).
 */
class HudiCOWScan(
                   spark: SparkSession,
                   metaClient: HoodieTableMetaClient,
                   fileIndex: HoodieFileIndex,
                   readSchema: StructType,
                   partFilters: Array[Filter],
                   options: CaseInsensitiveStringMap
                 ) extends Scan with Batch with SupportsReportStatistics {

  private lazy val broadcastHadoopConf: Broadcast[SerializableConfiguration] =

    spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sessionState.newHadoopConf())
    )

  // ── Scan ────────────────────────────────────────────────────────────────

  override def readSchema(): StructType = readSchema

  override def description(): String =
    s"HudiCOWScan[${metaClient.getTableConfig.getTableName}]" +
      s" schema=[${readSchema.fieldNames.mkString(",")}]" +
      s" filters=[${partFilters.mkString(",")}]"

  override def toBatch: Batch = this

  // ── Batch ────────────────────────────────────────────────────────────────

  override def planInputPartitions(): Array[InputPartition] = {
    // Convert DSv2 Filter to Catalyst Expression for HoodieFileIndex
    val catalystFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
      partFilters.map(HudiFilterConverter.toCatalyst).toSeq

    fileIndex.listFiles(catalystFilters, Seq.empty).flatMap { partDir =>
      partDir.files.map { status =>
        HudiCOWPartition(
          path            = status.getPath.toString,
          fileSize        = status.getLen,
          partitionValues = partDir.values
        )
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new HudiCOWReaderFactory(readSchema, broadcastHadoopConf)

  // ── SupportsReportStatistics ──────────────────────────────────────────────

  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = {
      val totalBytes = planInputPartitions()
        .collect { case p: HudiCOWPartition => p.fileSize }
        .sum
      OptionalLong.of(totalBytes)
    }
    // Row count needs the column stats metadata table — not available without it
    override def numRows(): OptionalLong = OptionalLong.empty()
  }
}

// ─── MOR Scan ─────────────────────────────────────────────────────────────────

/**
 * MOR Scan — for each file group produces one HudiMORPartition carrying
 * the base Parquet file (if any) and all log files up to latestInstant.
 *
 * Query types (controlled by hoodie.datasource.query.type):
 *   realtime (default) — base + log merge: fully current state
 *   snapshot           — base files only: slightly stale, no merge overhead
 *
 * Hudi 1.0.1 HoodieTableFileSystemView:
 *   Constructor: new HoodieTableFileSystemView(metaClient, timeline)
 *   Lazy — builds internal file group index on first access.
 *   getLatestFileSlices(partitionPath) returns Stream<FileSlice>.
 *   FileSlice.getBaseFile() returns Optional<HoodieBaseFile>.
 *   FileSlice.getLogFiles() returns Stream<HoodieLogFile> in desc order.
 *   HoodieLogFile.getPath() returns HoodieStoragePath in 1.0.1.
 *     Use .toString() to get the string path.
 */
class HudiMORScan(
                   spark: SparkSession,
                   metaClient: HoodieTableMetaClient,
                   fileIndex: HoodieFileIndex,
                   readSchema: StructType,
                   fullSchema: StructType,
                   partFilters: Array[Filter],
                   options: CaseInsensitiveStringMap
                 ) extends Scan with Batch {

  private val queryType: String =
    options.getOrDefault("hoodie.datasource.query.type", "realtime").toLowerCase

  private lazy val broadcastHadoopConf: Broadcast[SerializableConfiguration] =
    spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sessionState.newHadoopConf())
    )

  private lazy val latestInstant: String =
    metaClient.getActiveTimeline
      .getCommitsTimeline
      .filterCompletedInstants()
      .lastInstant()
      .map[String](_.getCompletionTime)
      .orElse("0")

  override def readSchema(): StructType = readSchema

  override def description(): String =
    s"HudiMORScan[${metaClient.getTableConfig.getTableName}][queryType=$queryType]"

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] =
    if (queryType == "snapshot") planSnapshot() else planRealtime()

  /**
   * Snapshot: base files only — same as COW, no log merging.
   */
  private def planSnapshot(): Array[InputPartition] = {
    val catalystFilters = partFilters.map(HudiFilterConverter.toCatalyst).toSeq
    fileIndex.listFiles(catalystFilters, Seq.empty).flatMap { partDir =>
      partDir.files.map { status =>
        HudiMORPartition(
          basePath          = Some(status.getPath.toString),
          logPaths          = Array.empty,
          fileGroupId       = fileGroupIdFromName(status.getPath.getName),
          partitionPath     = partDir.values.toString,
          latestInstantTime = latestInstant,
          fileSize          = status.getLen
        )
      }
    }.toArray
  }

  /**
   * Realtime: use HoodieTableFileSystemView to get (base, logs) per file group.
   *
   * Hudi 1.0.1:
   *   HoodieTableFileSystemView(metaClient, timeline) — lazy file listing
   *   getLatestFileSlices(partitionPath) — requires relative partition path
   *   HoodieLogFile.getPath().toString() — HoodieStoragePath.toString() gives full URI
   */
  private def planRealtime(): Array[InputPartition] = {
    val timeline = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()

    // Hudi 1.0.1: constructor lazily scans filesystem
    val engineContext: HoodieEngineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
      engineContext, metaClient, timeline
    )

    val catalystFilters = partFilters.map(HudiFilterConverter.toCatalyst).toSeq
    val partitionDirs   = fileIndex.listFiles(catalystFilters, Seq.empty)

    partitionDirs.flatMap { partDir =>
      // Convert Spark InternalRow partition values to relative path string
      // HoodieFileIndex stores relative partition paths internally
      val relPartPath = resolveRelativePartitionPath(partDir.files.map(_.fileStatus).toArray)

      fsView.getLatestFileSlices(relPartPath)
        .iterator()
        .asScala
        .map { slice =>
          // HoodieBaseFile.getPath() returns String in Hudi 1.0.1
          val baseFilePath = if (slice.getBaseFile.isPresent)
            Some(slice.getBaseFile.get().getPath)
          else None

          // HoodieLogFile.getPath() returns HoodieStoragePath — use toString
          val logFilePaths = slice.getLogFiles
            .iterator()
            .asScala
            .map(_.getPath.toString)
            .toArray

          val fileSize = baseFilePath
            .flatMap { p =>
              val hadoopPath = new org.apache.hadoop.fs.Path(p)
              val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
              scala.util.Try(fs.getFileStatus(hadoopPath).getLen).toOption
            }
            .getOrElse(0L)

          HudiMORPartition(
            basePath          = baseFilePath,
            logPaths          = logFilePaths,
            fileGroupId       = slice.getFileId,
            partitionPath     = relPartPath,
            latestInstantTime = latestInstant,
            fileSize          = fileSize
          )
        }.toSeq
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new HudiMORReaderFactory(
      readSchema          = readSchema,
      fullSchema          = fullSchema,
      basePath            = metaClient.getBasePath.toString,
      broadcastHadoopConf = broadcastHadoopConf,
      options             = options.asCaseSensitiveMap()
    )

  /**
   * Derives the relative partition path from the first FileStatus in a PartitionDirectory.
   * HoodieFileIndex lists files with full absolute paths — strip the table base path prefix.
   */
  private def resolveRelativePartitionPath(files: Array[FileStatus]): String = {
    if (files.isEmpty) return ""
    val fullPath  = files.head.getPath.getParent.toString
    val tableBase = metaClient.getBasePath.toString.stripSuffix("/")
    fullPath.stripPrefix(tableBase).stripPrefix("/")
  }

  private def fileGroupIdFromName(fileName: String): String =
    fileName.split("_").headOption.getOrElse(fileName)
}

// ─── Filter → Catalyst converter ─────────────────────────────────────────────

/**
 * Converts DSv2 Filter to Catalyst Expression for HoodieFileIndex.listFiles().
 * HoodieFileIndex expects Catalyst expressions for partition pruning.
 *
 * Spark 3.5.0: all imports from org.apache.spark.sql.catalyst.expressions.
 */
object HudiFilterConverter {
  import org.apache.spark.sql.catalyst.expressions._
  import org.apache.spark.sql.sources._

  def toCatalyst(filter: Filter): Expression = filter match {
    case org.apache.spark.sql.sources.EqualTo(attr,value)            => expressions.EqualTo(col(attr), Literal(value))
    case org.apache.spark.sql.sources.EqualNullSafe(attr, value)      => expressions.EqualNullSafe(col(attr), Literal(value))
    case org.apache.spark.sql.sources.GreaterThan(attr, value)        => expressions.GreaterThan(col(attr), Literal(value))
    case org.apache.spark.sql.sources.GreaterThanOrEqual(attr, value) => expressions.GreaterThanOrEqual(col(attr), Literal(value))
    case org.apache.spark.sql.sources.LessThan(attr, value)           => expressions.LessThan(col(attr), Literal(value))
    case org.apache.spark.sql.sources.LessThanOrEqual(attr, value)    => expressions.LessThanOrEqual(col(attr), Literal(value))
    case org.apache.spark.sql.sources.In(attr, values)                => expressions.In(col(attr), values.map(Literal(_)).toSeq)
    case org.apache.spark.sql.sources.IsNull(attr)                    => expressions.IsNull(col(attr))
    case org.apache.spark.sql.sources.IsNotNull(attr)                 => expressions.IsNotNull(col(attr))
    case org.apache.spark.sql.sources.And(left, right)               => expressions.And(toCatalyst(left), toCatalyst(right))
    case org.apache.spark.sql.sources.Or(left, right)                => expressions.Or(toCatalyst(left), toCatalyst(right))
    case org.apache.spark.sql.sources.Not(child)                     => expressions.Not(toCatalyst(child))
    case _                              => Literal(true) // unsupported → pass-through
  }

  private def col(name: String): UnresolvedAttribute = UnresolvedAttribute(name)
}
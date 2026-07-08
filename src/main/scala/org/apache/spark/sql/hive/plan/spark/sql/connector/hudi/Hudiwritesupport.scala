package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi


import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.util.{Option => HoodieOption}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable.ArrayBuffer

// ─── Non-row-level WriteBuilder (INSERT INTO / INSERT OVERWRITE) ─────────────

/**
 * Write builder for INSERT INTO and INSERT OVERWRITE operations.
 *
 * Spark 3.5.0:
 *   SupportsDynamicOverwrite — OVERWRITE with dynamic partition predicate
 *   SupportsOverwrite         — OVERWRITE with explicit filter
 */
// File-scope so HudiInsertWrite / HudiInsertBatchWrite can also see it — previously this was
// private to HudiWriteBuilder and never made it past build(), which is why INSERT OVERWRITE
// silently executed as a plain append (see HudiInsertBatchWrite below).
sealed trait OverwriteMode
case object NoOverwrite                       extends OverwriteMode
case object DynamicOverwrite                  extends OverwriteMode
case class  StaticOverwrite(f: Array[Filter]) extends OverwriteMode

class HudiWriteBuilder(
                        spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableType: HoodieTableType,
                        tableProps: Map[String, String],
                        info: LogicalWriteInfo
                      ) extends WriteBuilder
  with SupportsDynamicOverwrite
  with SupportsOverwrite {

  private var overwriteMode: OverwriteMode = NoOverwrite

  override def overwriteDynamicPartitions(): WriteBuilder = {
    overwriteMode = DynamicOverwrite; this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    overwriteMode = StaticOverwrite(filters); this
  }

  override def build(): Write = {
    val isDelta = tableType == HoodieTableType.MERGE_ON_READ
    new HudiInsertWrite(spark, metaClient, tableProps, info.schema(), isDelta, overwriteMode)
  }
}

class HudiInsertWrite(
                       spark: SparkSession,
                       metaClient: HoodieTableMetaClient,
                       tableProps: Map[String, String],
                       schema: StructType,
                       isDelta: Boolean,
                       overwriteMode: OverwriteMode
                     ) extends Write {
  override def toBatch: BatchWrite =
    new HudiInsertBatchWrite(spark, metaClient, tableProps, schema, isDelta, overwriteMode)
}

class HudiInsertBatchWrite(
                            spark: SparkSession,
                            metaClient: HoodieTableMetaClient,
                            tableProps: Map[String, String],
                            schema: StructType,
                            isDelta: Boolean,
                            overwriteMode: OverwriteMode
                          ) extends BatchWrite {

  // Overwrite (static or dynamic-partition) is represented in Hudi as a REPLACE_COMMIT,
  // carrying a partitionToReplaceFileIds map that tells the timeline which existing file
  // groups are superseded by this write. Plain INSERT/UPSERT stays on commit/deltacommit.
  private val isOverwrite = overwriteMode != NoOverwrite

  private val commitActionType: String =
    if (isOverwrite) HoodieTimeline.REPLACE_COMMIT_ACTION
    else if (isDelta) HoodieTimeline.DELTA_COMMIT_ACTION
    else HoodieTimeline.COMMIT_ACTION

  private val instantTime: String =
    HudiInstantUtils.createInflightInstant(metaClient, commitActionType)

  private val writeConfig: HoodieWriteConfig =
    HudiWriteConfigBuilder.build(metaClient, tableProps, schema)

  private val partitionPathField: String =
    tableProps.getOrElse("hoodie.datasource.write.partitionpath.field", "")

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HudiInsertWriterFactory(
      instantTime    = instantTime,
      writeConfig    = writeConfig,
      schema         = schema,
      recordKeyField = tableProps.getOrElse("hoodie.datasource.write.recordkey.field", ""),
      partitionField = partitionPathField
    )

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allRecords: Seq[HoodieRecord[_ <: HoodieRecordPayload[_]]] = messages
      .collect { case m: HudiRowLevelTaskMessage => m.records }
      .flatten.toSeq

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val recordsRdd = spark.sparkContext
        .parallelize(allRecords)
        .toJavaRDD()
        .asInstanceOf[org.apache.spark.api.java.JavaRDD[HoodieRecord[Nothing]]]

      // Previously this matched `HudiTaskCommitMessage` — a type the writer factory never
      // actually produced — so `allStatuses` was always empty and writeClient.commit() ran
      // against zero WriteStatus. Nothing was ever written to the base file/log for a plain
      // INSERT INTO. Fixed by driving the write here via writeClient.insert(), same as the
      // merge path drives writeClient.upsert() on its own buffered records.
      val statusRdd = writeClient.insert(recordsRdd, instantTime)
      val writtenPartitions = statusRdd.collect().asScala.map(_.getPartitionPath).distinct.toList

      val partitionToReplaceFileIds: ju.Map[String, ju.List[String]] =
        if (!isOverwrite) {
          ju.Collections.emptyMap[String, ju.List[String]]()
        } else {
          val targetPartitions = overwriteMode match {
            case DynamicOverwrite =>
              // Dynamic-partition overwrite: only replace the partitions that received new data,
              // matching Spark's OVERWRITE_DYNAMIC semantics.
              writtenPartitions
            case StaticOverwrite(filters) =>
              HudiOverwriteUtils.resolvePartitionsFromFilters(filters, partitionPathField) match {
                case Some(explicit) =>
                  // Static filter pinned to specific partition(s) — replace those even if this
                  // write produced no rows for one of them (e.g. `INSERT OVERWRITE ... WHERE p='x'`
                  // with an empty source is still expected to clear partition 'x').
                  (explicit ++ writtenPartitions).distinct
                case None =>
                  throw new UnsupportedOperationException(
                    "INSERT OVERWRITE with a filter on non-partition columns is not supported " +
                      "for Hudi tables in this catalog yet — only equality predicates on the " +
                      s"configured partition field(s) [$partitionPathField] can be translated " +
                      "into a Hudi partition-level replace. Use dynamic partition overwrite, " +
                      "or express the predicate purely in terms of the partition column(s)."
                  )
              }
            case NoOverwrite => Seq.empty // unreachable, isOverwrite guards this branch
          }
          HudiOverwriteUtils.existingFileIdsByPartition(metaClient, targetPartitions)
        }


      writeClient.commit(instantTime, statusRdd, HoodieOption.empty(), commitActionType,
        partitionToReplaceFileIds)

    } finally {
      writeClient.close()
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)
    try { writeClient.rollback(instantTime) } finally { writeClient.close() }
  }
}

/**
 * Helpers for translating Spark's V2 overwrite semantics into Hudi's replace-commit model.
 *
 * NOTE: written against the same Hudi 1.0.1/1.1.1 APIs the rest of this package targets
 * (HoodieTableFileSystemView.fileListingBasedFileSystemView, FileSlice.getFileId) — verify
 * against your local hudi-spark3.5-bundle jar before merging, the same way the surrounding
 * code's inline API comments were verified.
 */
object HudiOverwriteUtils {

  /**
   * Best-effort translation of SupportsOverwrite's Array[Filter] into a concrete set of
   * partition paths, when every filter is a simple equality predicate on the table's
   * (possibly composite) partition field(s). Returns None if any filter can't be mapped this
   * way, so the caller can fail loudly instead of silently ignoring the predicate.
   */
  def resolvePartitionsFromFilters(
                                    filters: Array[Filter],
                                    partitionPathField: String
                                  ): Option[Seq[String]] = {
    import org.apache.spark.sql.sources.EqualTo

    if (filters.isEmpty || partitionPathField.isEmpty) return None

    val partitionCols = partitionPathField.split(",").map(_.trim).filter(_.nonEmpty)
    if (partitionCols.isEmpty) return None

    val eqByCol = filters.collect { case EqualTo(attr, value) => attr -> String.valueOf(value) }.toMap
    if (eqByCol.size != filters.length) return None // some filter wasn't a plain equality
    if (!partitionCols.forall(eqByCol.contains)) return None // filter doesn't pin every partition column

    Some(Seq(partitionCols.map(eqByCol).mkString("/")))
  }

  /** All existing file-group IDs (base file IDs) in each of the given partitions, pre-write. */
  def existingFileIdsByPartition(
                                  metaClient: HoodieTableMetaClient,
                                  partitions: Seq[String]
                                ): ju.Map[String, ju.List[String]] = {
    if (partitions.isEmpty) return ju.Collections.emptyMap[String, ju.List[String]]()

    val fsView = org.apache.hudi.common.table.view.HoodieTableFileSystemView
      .fileListingBasedFileSystemView(
        new org.apache.hudi.client.common.HoodieSparkEngineContext(
          org.apache.spark.SparkContext.getOrCreate()),
        metaClient,
        metaClient.getActiveTimeline.filterCompletedInstants())

    val result = new ju.HashMap[String, ju.List[String]]()
    partitions.foreach { partitionPath =>
      val fileIds = fsView.getLatestFileSlices(partitionPath)
        .iterator().asScala
        .map(_.getFileId)
        .toList
        .asJava
      result.put(partitionPath, fileIds)
    }
    result
  }
}

class HudiInsertWriterFactory(
                               instantTime: String,
                               writeConfig: HoodieWriteConfig,
                               schema: StructType,
                               recordKeyField: String,
                               partitionField: String
                             ) extends DataWriterFactory {
  // NOTE: was `new HudiCOWMergeDataWriter(...)`, a class that lives in the now-deprecated
  // Hudicowmergewrite.scala (superseded by HudiRowLevelWrite.scala for MERGE/UPDATE/DELETE).
  // Plain INSERT/INSERT OVERWRITE has no business depending on that file at all — it happened
  // to work only because HudiCOWMergeDataWriter's `hasMeta` branch degrades gracefully for
  // plain-INSERT-shaped rows. Repointed at HudiRowLevelDataWriter (HudiRowLevelWrite.scala),
  // which has the identical buffering/key-detection shape and isn't marked for deletion.
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiRowLevelDataWriter(instantTime, schema, recordKeyField)
}

// ─── Shared: HoodieWriteConfig builder ───────────────────────────────────────

/**
 * Builds HoodieWriteConfig for Hudi 1.0.1.
 *
 * Key config keys in 1.0.1 (same string values as 0.x, different class locations):
 *   hoodie.insert.shuffle.parallelism   — parallelism for insert operations
 *   hoodie.upsert.shuffle.parallelism   — parallelism for upsert operations
 *   hoodie.delete.shuffle.parallelism   — parallelism for delete operations
 *   hoodie.index.type                   — BLOOM | HBASE | SIMPLE | GLOBAL_BLOOM
 *
 * withAutoCommit(false) is critical — we drive commit explicitly in BatchWrite.commit()
 * so that Spark's task commit protocol and driver-side collect() happen before committing.
 */
object HudiWriteConfigBuilder {

  def build(
             metaClient: HoodieTableMetaClient,
             tableProps: Map[String, String],
             schema: StructType
           ): HoodieWriteConfig = {
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
      schema, "hudi_record", "hoodie"
    )

    HoodieWriteConfig.newBuilder()
      .withPath(metaClient.getBasePath.toString)
      .withSchema(avroSchema.toString)
      .withParallelism(
        tableProps.getOrElse("hoodie.insert.shuffle.parallelism", "200").toInt,
        tableProps.getOrElse("hoodie.upsert.shuffle.parallelism", "200").toInt
      )
      .withDeleteParallelism(
        tableProps.getOrElse("hoodie.delete.shuffle.parallelism", "200").toInt
      )
      .forTable(metaClient.getTableConfig.getTableName)
      .withIndexConfig(
        HoodieIndexConfig.newBuilder()
          .withIndexType(
            HoodieIndex.IndexType.valueOf(
              tableProps.getOrElse("hoodie.index.type", "BLOOM")
            )
          )
          .build()
      ).withMetadataConfig(
        org.apache.hudi.common.config.HoodieMetadataConfig.newBuilder()
          .enable(tableProps.getOrElse("hoodie.metadata.enable", "false").toBoolean)
          .build()
      ) // We commit explicitly — do NOT change this
      .withProps(
        (tableProps ++ Map("hoodie.auto.commit" -> "false"))
          .asJava
          .asInstanceOf[java.util.Map[_, _]]
      )
      .build()
  }
}

// ─── Shared: Instant lifecycle management ────────────────────────────────────

/**
 * Hudi timeline instant management for Spark 3.5.0 + Hudi 1.0.1.
 *
 * Instant state transitions:
 *   REQUESTED → INFLIGHT → COMPLETED (commit/deltacommit)
 *              ↓
 *          ROLLBACK (on abort)
 *
 * Why create INFLIGHT before tasks start:
 *   Concurrent readers use the timeline to determine which instants are visible.
 *   An INFLIGHT instant signals a write-in-progress so readers can apply
 *   read-optimised query semantics (skip this instant's files for snapshot reads).
 *   Without it, partial task output could be read before the commit completes.
 *
 * Hudi 1.0.1 HoodieInstant constructors (verified from jar):
 *   new HoodieInstant(State, action, requestTime, Comparator<HoodieInstant>)
 *   new HoodieInstant(State, action, requestTime, completionTime, Comparator<HoodieInstant>)
 *   new HoodieInstant(State, action, requestTime, completionTime, isLegacy, Comparator<HoodieInstant>)
 *   Use HoodieInstant.COMPARATOR as the standard comparator.
 *
 * HoodieTimeline constants (Hudi 1.0.1):
 *   HoodieTimeline.COMMIT_ACTION        = "commit"
 *   HoodieTimeline.DELTA_COMMIT_ACTION  = "deltacommit"
 */
object HudiInstantUtils {

  /**
   * Creates a REQUESTED then transitions to INFLIGHT instant on the active timeline.
   *
   * @param metaClient loaded with setLoadActiveTimelineOnLoad(true)
   * @param isDelta    true = deltacommit (MOR), false = commit (COW)
   * @return instant time string (yyyyMMddHHmmssSSS format)
   */
  def createInflightInstant(
                             metaClient: HoodieTableMetaClient,
                             isDelta: Boolean
                           ): String = createInflightInstant(
    metaClient,
    if (isDelta) HoodieTimeline.DELTA_COMMIT_ACTION else HoodieTimeline.COMMIT_ACTION
  )

  /**
   * Same lifecycle as above but for an explicit action type — used for REPLACE_COMMIT_ACTION
   * on INSERT OVERWRITE, in addition to plain commit/deltacommit.
   */
  def createInflightInstant(
                             metaClient: HoodieTableMetaClient,
                             action: String
                           ): String = {
    // Confirmed from HoodieTableMetaClient source:
    //   createNewInstantTime() is an INSTANCE method on HoodieTableMetaClient in 1.0.1.
    //   It internally uses TimeGenerator with the table's timeGeneratorConfig.
    //   No longer static on HoodieActiveTimeline.
    val instantTime = metaClient.createNewInstantTime(false)
    val timeline = metaClient.getActiveTimeline

    // Confirmed from HoodieTableMetaClient source:
    //   createNewInstant(State, action, timestamp) is an INSTANCE method on HoodieTableMetaClient.
    //   It delegates to getInstantGenerator().createNewInstant(...) internally.
    //   No need to instantiate InstantGeneratorV2 directly.
    val requestedInstant = metaClient.createNewInstant(
      HoodieInstant.State.REQUESTED, action, instantTime
    )

    // Transition 1: write REQUESTED marker onto the timeline
    timeline.createNewInstant(requestedInstant)

    // Transition 2: REQUESTED → INFLIGHT
    timeline.transitionRequestedToInflight(requestedInstant, HoodieOption.empty[Array[Byte]]())

    instantTime
  }
}
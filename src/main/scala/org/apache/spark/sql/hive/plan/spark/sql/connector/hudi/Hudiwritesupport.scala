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
import scala.collection.mutable.ArrayBuffer

// ─── Non-row-level WriteBuilder (INSERT INTO / INSERT OVERWRITE) ─────────────

/**
 * Write builder for INSERT INTO and INSERT OVERWRITE operations.
 *
 * Spark 3.5.0:
 *   SupportsDynamicOverwrite — OVERWRITE with dynamic partition predicate
 *   SupportsOverwrite         — OVERWRITE with explicit filter
 */
class HudiWriteBuilder(
                        spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableType: HoodieTableType,
                        tableProps: Map[String, String],
                        info: LogicalWriteInfo
                      ) extends WriteBuilder
  with SupportsDynamicOverwrite
  with SupportsOverwrite {

  private sealed trait OverwriteMode
  private case object NoOverwrite                       extends OverwriteMode
  private case object DynamicOverwrite                  extends OverwriteMode
  private case class  StaticOverwrite(f: Array[Filter]) extends OverwriteMode

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
                       overwriteMode: Any
                     ) extends Write {
  override def toBatch: BatchWrite =
    new HudiInsertBatchWrite(spark, metaClient, tableProps, schema, isDelta)
}

class HudiInsertBatchWrite(
                            spark: SparkSession,
                            metaClient: HoodieTableMetaClient,
                            tableProps: Map[String, String],
                            schema: StructType,
                            isDelta: Boolean
                          ) extends BatchWrite {

  private val instantTime: String =
    HudiInstantUtils.createInflightInstant(metaClient, isDelta)

  private val writeConfig: HoodieWriteConfig =
    HudiWriteConfigBuilder.build(metaClient, tableProps, schema)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HudiInsertWriterFactory(instantTime, writeConfig, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allStatuses = messages
      .collect { case m: HudiTaskCommitMessage => m.writeStatuses }
      .flatten.toList

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)
    val action        = if (isDelta) HoodieTimeline.DELTA_COMMIT_ACTION else HoodieTimeline.COMMIT_ACTION

    try {
      val rdd = spark.sparkContext.parallelize(allStatuses)
      writeClient.commit(instantTime, rdd, HoodieOption.empty(), action,
        ju.Collections.emptyMap[String, ju.List[String]]())
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

class HudiInsertWriterFactory(
                               instantTime: String,
                               writeConfig: HoodieWriteConfig,
                               schema: StructType
                             ) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiCOWMergeDataWriter(instantTime, writeConfig, schema)
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
      )
      .withAutoCommit(false)            // We commit explicitly — do NOT change this
      .withProps(tableProps.asJava)
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
                           ): String = {
    // Confirmed from HoodieTableMetaClient source:
    //   createNewInstantTime() is an INSTANCE method on HoodieTableMetaClient in 1.0.1.
    //   It internally uses TimeGenerator with the table's timeGeneratorConfig.
    //   No longer static on HoodieActiveTimeline.
    val instantTime = metaClient.createNewInstantTime()

    val action   = if (isDelta) HoodieTimeline.DELTA_COMMIT_ACTION
    else HoodieTimeline.COMMIT_ACTION
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
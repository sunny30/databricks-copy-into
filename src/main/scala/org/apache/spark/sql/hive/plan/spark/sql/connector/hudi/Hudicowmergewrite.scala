package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{Option => HoodieOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

// ─── Shared commit message ────────────────────────────────────────────────────

/**
 * Per-task write result carried back to the driver for commit.
 *
 * Hudi 1.0.1: WriteStatus is in org.apache.hudi.client.WriteStatus (unchanged).
 * Each WriteStatus contains per-file write stats (path, records written, errors).
 */
case class HudiTaskCommitMessage(
                                  instantTime: String,
                                  writeStatuses: Seq[WriteStatus]
                                ) extends WriterCommitMessage

// ─── COW WriteBuilder ─────────────────────────────────────────────────────────

/**
 * Write builder for COW MERGE / UPDATE / DELETE (ReplaceData semantics).
 *
 * Spark 3.5 ReplaceData:
 *   Spark sends the full rewritten content of each affected file group.
 *   HudiCOWMergeWrite uses RequiresDistributionAndOrdering to cluster rows
 *   by _hoodie_file_name, guaranteeing one task owns one file group completely.
 */
class HudiCOWMergeWriteBuilder(
                                spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                tableProps: Map[String, String],
                                info: LogicalWriteInfo
                              ) extends WriteBuilder {

  override def build(): Write =
    new HudiCOWMergeWrite(spark, metaClient, tableProps, info.schema())
}

class HudiCOWMergeWrite(
                         spark: SparkSession,
                         metaClient: HoodieTableMetaClient,
                         tableProps: Map[String, String],
                         schema: StructType
                       ) extends Write with RequiresDistributionAndOrdering {

  /**
   * Cluster all rows for the same file group to the same task.
   * Required for COW correctness: a task must hold ALL rows for a file group
   * to produce a complete and correct Parquet rewrite.
   *
   * Spark 3.5: FieldReference.column() in org.apache.spark.sql.connector.expressions
   * Distributions.clustered() in org.apache.spark.sql.connector.distributions
   */
  override def requiredDistribution(): Distribution =
    Distributions.clustered(Array(FieldReference.column("_hoodie_file_name")))

  override def requiredOrdering(): Array[SortOrder] = Array.empty

  override def toBatch: BatchWrite =
    new HudiCOWMergeBatchWrite(spark, metaClient, tableProps, schema)
}

// ─── COW BatchWrite ───────────────────────────────────────────────────────────

class HudiCOWMergeBatchWrite(
                              spark: SparkSession,
                              metaClient: HoodieTableMetaClient,
                              tableProps: Map[String, String],
                              schema: StructType
                            ) extends BatchWrite {

  // Create inflight instant BEFORE task execution so concurrent readers see this write
  private val instantTime: String =
    HudiInstantUtils.createInflightInstant(metaClient, isDelta = false)

  private val writeConfig: HoodieWriteConfig =
    HudiWriteConfigBuilder.build(metaClient, tableProps, schema)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HudiCOWMergeWriterFactory(instantTime, writeConfig, schema)

  /**
   * Driver-side commit.
   * Collects WriteStatus from all tasks and commits via SparkRDDWriteClient.
   *
   * Hudi 1.0.1: SparkRDDWriteClient no longer has explicit HoodieRecordPayload generic.
   * commit() signature: commit(instantTime, writeStatusRDD, extraMetadata, actionType, partitionToReplacedFileIds)
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allStatuses = messages
      .collect { case m: HudiTaskCommitMessage => m.writeStatuses }
      .flatten.toList

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    // Hudi 1.0.1: raw type — no HoodieRecordPayload generic parameter
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val statusRdd = spark.sparkContext.parallelize(allStatuses)
      writeClient.commit(
        instantTime,
        statusRdd,
        HoodieOption.empty(),
        HoodieTimeline.COMMIT_ACTION,
        ju.Collections.emptyMap[String, ju.List[String]]()
      )
    } catch {
      case e: Exception =>
        writeClient.rollback(instantTime)
        throw new RuntimeException(s"COW commit failed at $instantTime", e)
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

// ─── COW Writer Factory ───────────────────────────────────────────────────────

class HudiCOWMergeWriterFactory(
                                 instantTime: String,
                                 writeConfig: HoodieWriteConfig,
                                 schema: StructType
                               ) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiCOWMergeDataWriter(instantTime, writeConfig, schema)
}

// ─── COW DataWriter ───────────────────────────────────────────────────────────

/**
 * Per-task COW data writer.
 *
 * Converts InternalRow → HoodieAvroRecord via Spark 3.5 AvroSerializer,
 * buffers records, then drives SparkRDDWriteClient.upsert() at commit time.
 *
 * Spark 3.5 AvroSerializer:
 *   org.apache.spark.sql.avro.AvroSerializer
 *   new AvroSerializer(catalystType: StructType, avroType: Schema, nullable: Boolean)
 *   serialize(catalyst: Any): Any  — returns GenericRecord
 *
 * Hudi 1.0.1 write path:
 *   HoodieAvroRecord(HoodieKey, HoodieRecordPayload)   — payload carries the Avro record
 *   OverwriteWithLatestAvroPayload(GenericRecord, orderingVal: Long)
 *   SparkRDDWriteClient.upsert(JavaRDD<HoodieRecord<?>>, instantTime) → JavaRDD<WriteStatus>
 */
class HudiCOWMergeDataWriter(
                              instantTime: String,
                              writeConfig: HoodieWriteConfig,
                              schema: StructType
                            ) extends DataWriter[InternalRow] {

  private val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
    schema, "hudi_record", "hoodie"
  )

  // Spark 3.5 AvroSerializer: (StructType, Schema, nullable: Boolean)
  private val avroSerializer = new AvroSerializer(schema, avroSchema, nullable = false)

  private val recordKeyIdx     = schema.fieldIndex("_hoodie_record_key")
  private val partitionPathIdx = schema.fieldIndex("_hoodie_partition_path")

  // Ordering value column (optional — defaults to 0 if not present)
  private val orderingValIdx: Option[Int] =
    if (schema.fieldNames.contains("_hoodie_commit_time"))
      Some(schema.fieldIndex("_hoodie_commit_time"))
    else None

  private val recordBuffer = ArrayBuffer.empty[HoodieRecord[_ <: HoodieRecordPayload[_]]]

  override def write(row: InternalRow): Unit = {
    val avroRecord    = avroSerializer.serialize(row).asInstanceOf[GenericRecord]
    val recordKey     = row.getString(recordKeyIdx)
    val partitionPath = row.getString(partitionPathIdx)
    val orderingVal   = orderingValIdx.map(i => row.getLong(i)).getOrElse(0L)

    val hoodieKey = new HoodieKey(recordKey, partitionPath)
    val payload   = new OverwriteWithLatestAvroPayload(avroRecord, orderingVal)
    recordBuffer  += new HoodieAvroRecord(hoodieKey, payload)
  }

  override def commit(): WriterCommitMessage = {
    val sc            = SparkContext.getOrCreate()
    val engineContext = new HoodieSparkEngineContext(sc)
    // Hudi 1.0.1: raw SparkRDDWriteClient, no explicit payload generic
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val recordsJavaRdd = sc.parallelize(recordBuffer)
      // upsert returns JavaRDD[WriteStatus]
      val statuses = writeClient.upsert(recordsJavaRdd.toJavaRDD().asInstanceOf[JavaRDD[HoodieRecord[Nothing]]], instantTime).collect().toSeq
      HudiTaskCommitMessage(instantTime, statuses)
    } finally {
      writeClient.close()
      recordBuffer.clear()
    }
  }

  override def abort(): Unit  = recordBuffer.clear()
  override def close(): Unit  = recordBuffer.clear()
}
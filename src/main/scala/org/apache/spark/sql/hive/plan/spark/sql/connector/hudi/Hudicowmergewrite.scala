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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// ─── Shared commit message ────────────────────────────────────────────────────

/**
 * Per-task write result carried back to the driver for commit.
 *
 * Hudi 1.0.1: WriteStatus is in org.apache.hudi.client.WriteStatus (unchanged).
 * Each WriteStatus contains per-file write stats (path, records written, errors).
 */
// Executor → Driver: buffered records to be upserted by the driver
// SparkRDDWriteClient must NEVER be created on an executor
case class HudiCOWRecordsMessage(
                                  instantTime: String,
                                  records: Seq[HoodieRecord[_ <: HoodieRecordPayload[_]]]
                                ) extends WriterCommitMessage

// Driver-internal: carries WriteStatus after driver-side commit
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
    new HudiCOWMergeWriterFactory(
      instantTime    = instantTime,
      writeConfig    = writeConfig,
      schema         = schema,
      recordKeyField = tableProps.getOrElse("hoodie.datasource.write.recordkey.field", ""),
      partitionField = tableProps.getOrElse("hoodie.datasource.write.partitionpath.field", "")
    )

  /**
   * Driver-side commit.
   * Collects WriteStatus from all tasks and commits via SparkRDDWriteClient.
   *
   * Hudi 1.0.1: SparkRDDWriteClient no longer has explicit HoodieRecordPayload generic.
   * commit() signature: commit(instantTime, writeStatusRDD, extraMetadata, actionType, partitionToReplacedFileIds)
   */
  // Driver-side: collects records from all executor tasks, upserts via write client.
  // SparkRDDWriteClient is safe here — BatchWrite.commit() always runs on the driver.
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allRecords: Seq[HoodieRecord[_ <: HoodieRecordPayload[_]]] = messages
      .collect { case m: HudiCOWRecordsMessage => m.records }
      .flatten
      .toSeq

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val recordsRdd = spark.sparkContext
        .parallelize(allRecords)
        .toJavaRDD()
        .asInstanceOf[org.apache.spark.api.java.JavaRDD[HoodieRecord[Nothing]]]

      val statusRdd = writeClient.upsert(recordsRdd, instantTime)

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
                                 schema: StructType,
                                 recordKeyField: String,
                                 partitionField: String
                               ) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiCOWMergeDataWriter(instantTime, schema, recordKeyField, partitionField)
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
                              schema: StructType,
                              recordKeyField: String,  // e.g. "id" — the user column that is the record key
                              partitionField: String   // e.g. "city" — the user column used for partitioning
                            ) extends DataWriter[InternalRow] {

  private val avroSchema     = AvroConversionUtils.convertStructTypeToAvroSchema(
    schema, "hudi_record", "hoodie"
  )
  private val avroSerializer = new AvroSerializer(schema, avroSchema, nullable = false)

  // ── Determine how to extract record key and partition path ──────────────────
  //
  // Two incoming row shapes depending on operation:
  //
  //  INSERT INTO → Spark sends user columns only (e.g. id, city)
  //    _hoodie_record_key is NOT present — derive from configured recordKeyField
  //
  //  MERGE / UPDATE / DELETE → Spark sends full schema including meta fields
  //    _hoodie_record_key IS present — read directly (already populated by prior scan)
  //
  // We detect which case we are in once at construction time by checking the schema.

  private val hasMeta: Boolean = schema.fieldNames.contains("_hoodie_record_key")

  // INSERT path: index of user-defined record key column in incoming schema
  private val userRecordKeyIdx: Option[Int] =
    if (!hasMeta && recordKeyField.nonEmpty && schema.fieldNames.contains(recordKeyField))
      Some(schema.fieldIndex(recordKeyField))
    else None

  // INSERT path: index of user-defined partition column in incoming schema
  private val userPartitionIdx: Option[Int] =
    if (!hasMeta && partitionField.nonEmpty && schema.fieldNames.contains(partitionField))
      Some(schema.fieldIndex(partitionField))
    else None

  // MERGE path: meta field indices (present only when hasMeta = true)
  private val metaRecordKeyIdx: Option[Int]  =
    if (hasMeta) Some(schema.fieldIndex("_hoodie_record_key")) else None
  private val metaPartitionIdx: Option[Int]  =
    if (hasMeta) Some(schema.fieldIndex("_hoodie_partition_path")) else None
  private val metaOrderingIdx:  Option[Int]  =
    if (hasMeta && schema.fieldNames.contains("_hoodie_commit_time"))
      Some(schema.fieldIndex("_hoodie_commit_time"))
    else None

  private val recordBuffer = ArrayBuffer.empty[HoodieRecord[_ <: HoodieRecordPayload[_]]]

  override def write(row: InternalRow): Unit = {
    val avroRecord = avroSerializer.serialize(row).asInstanceOf[GenericRecord]

    val (recordKey, partitionPath, orderingVal) = if (hasMeta) {
      // MERGE / UPDATE / DELETE: meta fields already in the row
      val rk   = metaRecordKeyIdx.map(i => row.getString(i)).getOrElse("")
      val pp   = metaPartitionIdx.map(i => row.getString(i)).getOrElse("")
      val ord  = metaOrderingIdx.map(i => row.getLong(i)).getOrElse(0L)
      (rk, pp, ord)
    } else {
      // INSERT INTO: derive from user columns
      val rk  = userRecordKeyIdx
        .map(i => row.get(i, schema(i).dataType).toString)
        .getOrElse(java.util.UUID.randomUUID().toString)  // fallback: UUID if no key field set
      val pp  = userPartitionIdx
        .map(i => row.get(i, schema(i).dataType).toString)
        .getOrElse("")
      (rk, pp, 0L)
    }

    val hoodieKey = new HoodieKey(recordKey, partitionPath)
    val payload   = new OverwriteWithLatestAvroPayload(avroRecord, orderingVal)
    recordBuffer += new HoodieAvroRecord[OverwriteWithLatestAvroPayload](hoodieKey, payload)
  }

  // Executor-side commit: return buffered records to the driver.
  // NEVER create SparkContext/SparkSession/HoodieSparkEngineContext here —
  // all are driver-only and throw IllegalStateException on executors.
  override def commit(): WriterCommitMessage = {
    val result = HudiCOWRecordsMessage(instantTime, recordBuffer.toSeq)
    recordBuffer.clear()
    result
  }

  override def abort(): Unit  = recordBuffer.clear()
  override def close(): Unit  = recordBuffer.clear()
}
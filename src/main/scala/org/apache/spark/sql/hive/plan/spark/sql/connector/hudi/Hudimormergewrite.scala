package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi



import org.apache.avro.generic.GenericRecord
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model._
import org.apache.hudi.common.model.{HoodieAvroRecord, HoodieKey, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.{HoodieAvroDataBlock, HoodieDeleteBlock, HoodieLogBlock}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.util.{Option => HoodieOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * MOR write builder for row-level operations.
 * Spark 3.5.0 + Hudi 1.0.1.
 *
 * WriteDelta semantics (MOR):
 *   Spark sends only the changed rows (deltas).
 *   Changed rows → new log block appended to the file group's log file.
 *   Deleted rows → DELETE_BLOCK appended to the log.
 *   Base Parquet files are NOT rewritten — compaction handles that async.
 *
 * Hudi 1.0.1 log writer changes vs 0.x:
 *   HoodieLogFormat.newWriterBuilder()
 *     .withStorage(HoodieStorage)           ← replaces .withFs(FileSystem)
 *     .onParentPath(HoodieStoragePath)      ← replaces hadoop Path
 *   HoodieAvroDataBlock constructor (verified): (List<HoodieRecord>, Map<HeaderMetadataType,String>, String keyField)
 *   HoodieDeleteBlock constructor:   (deletedRecords, copyOnWriteOperations, header)
 *   DeleteRecord.create(key, partitionPath) ← static factory, not new constructor
 */
class HudiMORMergeWriteBuilder(
                                spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                tableProps: Map[String, String],
                                info: LogicalWriteInfo
                              ) extends WriteBuilder {

  override def build(): Write =
    new HudiMORMergeWrite(spark, metaClient, tableProps, info.schema())
}

class HudiMORMergeWrite(
                         spark: SparkSession,
                         metaClient: HoodieTableMetaClient,
                         tableProps: Map[String, String],
                         schema: StructType
                       ) extends Write {

  // MOR WriteDelta: no RequiresDistributionAndOrdering.
  // Multiple tasks can write to different log file versions of the same file group.
  // Hudi's log file version numbering prevents conflicts.
  override def toBatch: BatchWrite =
    new HudiMORMergeBatchWrite(spark, metaClient, tableProps, schema)
}

// ─── MOR BatchWrite ───────────────────────────────────────────────────────────

class HudiMORMergeBatchWrite(
                              spark: SparkSession,
                              metaClient: HoodieTableMetaClient,
                              tableProps: Map[String, String],
                              schema: StructType
                            ) extends BatchWrite {

  // MOR uses deltacommit — create inflight instant with isDelta=true
  private val instantTime: String =
    HudiInstantUtils.createInflightInstant(metaClient, isDelta = true)

  private val writeConfig: HoodieWriteConfig =
    HudiWriteConfigBuilder.build(metaClient, tableProps, schema)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HudiMORMergeWriterFactory(
      instantTime    = instantTime,
      writeConfig    = writeConfig,
      schema         = schema,
      basePath       = metaClient.getBasePath.toString,
      hadoopConf     = spark.sessionState.newHadoopConf(),
      recordKeyField = tableProps.getOrElse("hoodie.datasource.write.recordkey.field", ""),
      partitionField = tableProps.getOrElse("hoodie.datasource.write.partitionpath.field", "")
    )

  /**
   * Driver-side deltacommit.
   *
   * Hudi 1.0.1: DELTA_COMMIT_ACTION for MOR (COMMIT_ACTION is COW only).
   * commit() collects WriteStatus from all tasks.
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allStatuses = messages
      .collect { case m: HudiTaskCommitMessage => m.writeStatuses }
      .flatten.toList

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val statusRdd = spark.sparkContext.parallelize(allStatuses)
      writeClient.commit(
        instantTime,
        statusRdd,
        HoodieOption.empty(),
        HoodieTimeline.DELTA_COMMIT_ACTION,              // MOR: deltacommit
        ju.Collections.emptyMap[String, ju.List[String]]()
      )
    } catch {
      case e: Exception =>
        writeClient.rollback(instantTime)
        throw new RuntimeException(s"MOR delta commit failed at $instantTime", e)
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

// ─── MOR Writer Factory ───────────────────────────────────────────────────────

class HudiMORMergeWriterFactory(
                                 instantTime: String,
                                 writeConfig: HoodieWriteConfig,
                                 schema: StructType,
                                 basePath: String,
                                 hadoopConf: org.apache.hadoop.conf.Configuration,
                                 recordKeyField: String,
                                 partitionField: String
                               ) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiMORMergeDataWriter(instantTime, writeConfig, schema, basePath, hadoopConf, recordKeyField, partitionField)
}

// ─── MOR DataWriter ───────────────────────────────────────────────────────────

/**
 * Per-task MOR data writer.
 *
 * Groups incoming rows by (partitionPath, fileGroupId).
 * For each group:
 *   Upsert rows → HoodieAvroDataBlock appended to log
 *   Delete rows → HoodieDeleteBlock  appended to log
 *
 * Hudi 1.0.1 log writing:
 *   HoodieLogFormat.newWriterBuilder()
 *     .onParentPath(HoodieStoragePath)   — Hudi storage path, not hadoop Path
 *     .withStorage(HoodieHadoopStorage)  — Hudi storage abstraction
 *     .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
 *     .withFileId(fileGroupId)
 *     .overBaseCommit(instantTime)
 *     .build()
 *
 * DeleteRecord.create() in Hudi 1.0.1 (verified):
 *   static factory: DeleteRecord.create(recordKey, partitionPath)
 *
 * HoodieDeleteBlock constructor in 1.0.1 (verified from jar):
 *   new HoodieDeleteBlock(List<Pair<DeleteRecord, Long>>, Map<HeaderMetadataType, String>)
 *   Pair = org.apache.hudi.common.util.collection.Pair
 *   Long = ordering value for conflict resolution during compaction
 *
 * HoodieAvroDataBlock constructor in 1.0.1 (verified from jar):
 *   new HoodieAvroDataBlock(List<HoodieRecord>, Map<HeaderMetadataType,String>, String keyField)
 *   keyField = the record key field name, used during log scan to extract record keys
 *
 * Spark 3.5 AvroSerializer:
 *   new AvroSerializer(schema, avroSchema, nullable)
 *   serialize(row) returns GenericRecord
 */
class HudiMORMergeDataWriter(
                              instantTime: String,
                              writeConfig: HoodieWriteConfig,
                              schema: StructType,
                              basePath: String,
                              hadoopConf: org.apache.hadoop.conf.Configuration,
                              recordKeyField: String,  // user column name for record key, passed to HoodieAvroDataBlock
                              partitionField: String   // user column name for partition path
                            ) extends DataWriter[InternalRow] {

  private val avroSchema     = AvroConversionUtils.convertStructTypeToAvroSchema(
    schema, "hudi_record", "hoodie"
  )
  private val avroSerializer = new AvroSerializer(schema, avroSchema, nullable = false)

  // ── Detect incoming row shape ─────────────────────────────────────────────
  // INSERT INTO → user columns only (id, city) — no _hoodie_* meta fields
  // MERGE / UPDATE / DELETE → full schema including _hoodie_* meta fields
  private val hasMeta: Boolean = schema.fieldNames.contains("_hoodie_record_key")

  // INSERT path: user column indices
  private val userRecordKeyIdx: Option[Int] =
    if (!hasMeta && recordKeyField.nonEmpty && schema.fieldNames.contains(recordKeyField))
      Some(schema.fieldIndex(recordKeyField))
    else None

  private val userPartitionIdx: Option[Int] =
    if (!hasMeta && partitionField.nonEmpty && schema.fieldNames.contains(partitionField))
      Some(schema.fieldIndex(partitionField))
    else None

  // MERGE path: meta field indices
  private val metaRecordKeyIdx: Option[Int] =
    if (hasMeta) Some(schema.fieldIndex("_hoodie_record_key")) else None
  private val metaPartitionIdx: Option[Int] =
    if (hasMeta) Some(schema.fieldIndex("_hoodie_partition_path")) else None
  private val fileNameIdx: Int =
    if (hasMeta && schema.fieldNames.contains("_hoodie_file_name"))
      schema.fieldIndex("_hoodie_file_name")
    else -1

  // Ordering value — from meta field if present, else 0L
  private val orderingValIdx: Option[Int] =
    if (hasMeta && schema.fieldNames.contains("_hoodie_commit_time"))
      Some(schema.fieldIndex("_hoodie_commit_time"))
    else None

  // Detect delete rows — Spark marks deleted rows with _hoodie_is_deleted = true in WriteDelta
  // Only relevant in the MERGE path (hasMeta = true)
  private val isDeletedIdx: Int =
  if (hasMeta && schema.fieldNames.contains("_hoodie_is_deleted"))
    schema.fieldIndex("_hoodie_is_deleted")
  else -1

  // groupKey = "partitionPath|fileGroupId"
  // Buffer stores HoodieAvroRecord — required by HoodieAvroDataBlock(List<HoodieRecord>, header, keyField)
  private val upsertBuffer = mutable.Map.empty[String, mutable.ArrayBuffer[HoodieRecord[OverwriteWithLatestAvroPayload]]]
  // (recordKey, partitionPath, orderingVal)
  private val deleteBuffer = mutable.Map.empty[String, mutable.ArrayBuffer[(String, String, Long)]]

  override def write(row: InternalRow): Unit = {
    val (recordKey, partitionPath) = if (hasMeta) {
      // MERGE / UPDATE / DELETE — meta fields already in the row
      val rk = metaRecordKeyIdx.map(i => row.getString(i)).getOrElse("")
      val pp = metaPartitionIdx.map(i => row.getString(i)).getOrElse("")
      (rk, pp)
    } else {
      // INSERT INTO — derive from user columns
      val rk = userRecordKeyIdx
        .map(i => row.get(i, schema(i).dataType).toString)
        .getOrElse(ju.UUID.randomUUID().toString)
      val pp = userPartitionIdx
        .map(i => row.get(i, schema(i).dataType).toString)
        .getOrElse("")
      (rk, pp)
    }

    val fileGroupId = if (fileNameIdx >= 0) fileGroupIdFromFileName(row.getString(fileNameIdx))
    else ju.UUID.randomUUID().toString
    val groupKey    = s"$partitionPath|$fileGroupId"
    val isDelete    = isDeletedIdx >= 0 && !row.isNullAt(isDeletedIdx) && row.getBoolean(isDeletedIdx)

    if (isDelete) {
      val orderingVal = orderingValIdx.map(i => row.getLong(i)).getOrElse(0L)
      deleteBuffer.getOrElseUpdate(groupKey, mutable.ArrayBuffer.empty) +=
        ((recordKey, partitionPath, orderingVal))
    } else {
      val avroRecord   = avroSerializer.serialize(row).asInstanceOf[GenericRecord]
      val orderingVal  = orderingValIdx.map(i => row.getLong(i)).getOrElse(0L)
      val hoodieKey    = new HoodieKey(recordKey, partitionPath)
      val payload      = new OverwriteWithLatestAvroPayload(avroRecord, orderingVal)
      val hoodieRecord = new HoodieAvroRecord[OverwriteWithLatestAvroPayload](hoodieKey, payload)
      upsertBuffer.getOrElseUpdate(groupKey, mutable.ArrayBuffer.empty) += hoodieRecord
    }
  }

  override def commit(): WriterCommitMessage = {
    val statuses = mutable.ArrayBuffer.empty[WriteStatus]

    upsertBuffer.foreach { case (groupKey, records) =>
      val (partitionPath, fileGroupId) = splitGroupKey(groupKey)
      statuses += writeAvroBlock(partitionPath, fileGroupId, records.toList)
    }
    deleteBuffer.foreach { case (groupKey, keys) =>
      val (partitionPath, fileGroupId) = splitGroupKey(groupKey)
      statuses += writeDeleteBlock(partitionPath, fileGroupId, keys.toList)
    }

    HudiTaskCommitMessage(instantTime, statuses.toSeq)
  }

  override def abort(): Unit = { upsertBuffer.clear(); deleteBuffer.clear() }
  override def close(): Unit = { upsertBuffer.clear(); deleteBuffer.clear() }

  // ─── Log block writers ────────────────────────────────────────────────────

  private def writeAvroBlock(
                              partitionPath: String,
                              fileGroupId: String,
                              records: List[HoodieRecord[OverwriteWithLatestAvroPayload]]
                            ): WriteStatus = {
    val status    = new WriteStatus()
    val logWriter = openLogWriter(partitionPath, fileGroupId)
    try {
      val header = buildHeader(HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK)
      // Hudi 1.0.1 HoodieAvroDataBlock constructor (verified from jar):
      //   HoodieAvroDataBlock(List<HoodieRecord>, Map<HeaderMetadataType, String>, String keyField)
      // keyField = the record key field name (e.g. "order_id") — used by the block
      // to extract record keys when the block is read back during log scanning.
      // java.util.List is invariant in its type parameter, so Scala cannot
      // automatically widen List[HoodieRecord[OverwriteWithLatestAvroPayload]]
      // to List[HoodieRecord[_]] even though the element type is a subtype.
      // Cast explicitly at the Java API boundary — safe because generics are
      // erased at runtime and HoodieAvroDataBlock never inspects the payload type.
      val recordsWildcard = records.asJava
        .asInstanceOf[java.util.List[HoodieRecord[_]]]

      val block = new HoodieAvroDataBlock(recordsWildcard, header, recordKeyField)
      logWriter.appendBlock(block)
      status.setFileId(fileGroupId)
      status.setPartitionPath(partitionPath)
    } catch {
      case e: Exception => status.setGlobalError(e)
    } finally {
      logWriter.close()
    }
    status
  }

  private def writeDeleteBlock(
                                partitionPath: String,
                                fileGroupId: String,
                                keys: List[(String, String, Long)]  // (recordKey, partitionPath, orderingVal)
                              ): WriteStatus = {
    val status    = new WriteStatus()
    val logWriter = openLogWriter(partitionPath, fileGroupId)
    try {
      val header = buildHeader(HoodieLogBlock.HoodieLogBlockType.DELETE_BLOCK)

      // Hudi 1.0.1 actual constructor (verified from jar):
      //   HoodieDeleteBlock(List<Pair<DeleteRecord, Long>> recordsToDelete,
      //                     Map<HeaderMetadataType, String> header)
      // Pair is org.apache.hudi.common.util.collection.Pair
      // The Long is the ordering value used during log compaction to resolve
      // conflicts when the same key appears in multiple delete blocks.
      val deleteRecords: java.util.List[org.apache.hudi.common.util.collection.Pair[DeleteRecord, java.lang.Long]] =
      keys.map { case (key, part, orderingVal) =>
        org.apache.hudi.common.util.collection.Pair.of(
          DeleteRecord.create(key, part),
          java.lang.Long.valueOf(orderingVal)
        )
      }.asJava

      val block = new HoodieDeleteBlock(deleteRecords, header)
      logWriter.appendBlock(block)
      status.setFileId(fileGroupId)
      status.setPartitionPath(partitionPath)
    } catch {
      case e: Exception => status.setGlobalError(e)
    } finally {
      logWriter.close()
    }
    status
  }

  /**
   * Hudi 1.0.1 log writer:
   *   .withStorage(HoodieHadoopStorage) replaces .withFs(FileSystem)
   *   .onParentPath(HoodieStoragePath)  replaces hadoop Path
   */
  private def openLogWriter(partitionPath: String, fileGroupId: String): HoodieLogFormat.Writer = {
    val partDir     = if (partitionPath.isEmpty) basePath else s"$basePath/$partitionPath"
    val storagePath = new StoragePath(partDir)
    val storage     = new HoodieHadoopStorage(storagePath, new HadoopStorageConfiguration(hadoopConf))

    HoodieLogFormat.newWriterBuilder()
      .onParentPath(storagePath)                          // Hudi 1.0.1: HoodieStoragePath
      .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
      .withFileId(fileGroupId)
      .withStorage(storage)                               // Hudi 1.0.1: HoodieStorage
      .build()
  }

  private def buildHeader(
                           blockType: HoodieLogBlock.HoodieLogBlockType
                         ): ju.Map[HoodieLogBlock.HeaderMetadataType, String] = {
    val h = new ju.HashMap[HoodieLogBlock.HeaderMetadataType, String]()
    h.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, instantTime)
    h.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, avroSchema.toString)
    h.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, instantTime)
    h
  }

  private def fileGroupIdFromFileName(name: String): String =
    name.split("_").headOption.getOrElse(name)

  private def splitGroupKey(key: String): (String, String) = {
    val i = key.indexOf('|')
    (key.substring(0, i), key.substring(i + 1))
  }
}
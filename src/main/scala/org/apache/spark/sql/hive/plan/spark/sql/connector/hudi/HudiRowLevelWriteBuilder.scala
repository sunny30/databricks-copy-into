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
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, SortOrder}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Replaces HudiCOWMergeWrite / HudiMORMergeWrite.
 *
 * Why one class instead of two: SparkRDDWriteClient.upsert() and .delete() already dispatch
 * to the correct physical behavior (rewrite the base parquet file for COW, append a log block
 * for MOR) purely by reading metaClient.getTableConfig().getTableType() — that dispatch is
 * Hudi's job, driven entirely by the table's path + timeline, not something this catalog layer
 * needs to duplicate. The old HudiMORMergeWrite hand-rolled HoodieLogFormat/HoodieDeleteBlock
 * writing, which is exactly what SparkRDDWriteClient.delete() already does internally.
 *
 * Why DELETE needed more than "just write what Spark sends": HudiBaseRowLevelOperation does
 * NOT implement SupportsDelta, so Spark always plans MERGE/UPDATE/DELETE as ReplaceData — the
 * write side only ever receives the *surviving* rows per touched file group, never an explicit
 * "this row was deleted" signal. Rather than inventing a signal, deletedKeys is computed by
 * reading the CURRENT keys of each touched partition straight from the path/timeline — via the
 * exact same HudiScanBuilder used for ordinary reads — and subtracting the keys this write
 * actually saw. Whatever's missing was deleted. No Spark-side bookkeeping required.
 */
class HudiRowLevelWriteBuilder(
                                spark: SparkSession,
                                metaClient: HoodieTableMetaClient,
                                tableType: HoodieTableType,
                                tableProps: Map[String, String],
                                catalogName: String,
                                info: LogicalWriteInfo
                              ) extends WriteBuilder {

  override def build(): Write =
    new HudiRowLevelWrite(spark, metaClient, tableType, tableProps, catalogName, info.schema())
}

class HudiRowLevelWrite(
                         spark: SparkSession,
                         metaClient: HoodieTableMetaClient,
                         tableType: HoodieTableType,
                         tableProps: Map[String, String],
                         catalogName: String,
                         schema: StructType
                       ) extends Write with RequiresDistributionAndOrdering {

  // Still cluster by file group — keeps one task's buffered records aligned with one file
  // group, which keeps the driver-side deletedKeys diff (per partition) cheap and correct
  // regardless of task count.
  override def requiredDistribution(): Distribution =
    Distributions.clustered(Array(FieldReference.column("_hoodie_file_name")))

  override def requiredOrdering(): Array[SortOrder] = Array.empty

  override def toBatch: BatchWrite =
    new HudiRowLevelBatchWrite(spark, metaClient, tableType, tableProps, catalogName, schema)
}

class HudiRowLevelBatchWrite(
                              spark: SparkSession,
                              metaClient: HoodieTableMetaClient,
                              tableType: HoodieTableType,
                              tableProps: Map[String, String],
                              catalogName: String,
                              schema: StructType
                            ) extends BatchWrite {

  private val instantTime: String =
    HudiInstantUtils.createInflightInstant(metaClient, tableType == HoodieTableType.MERGE_ON_READ)

  private val writeConfig: HoodieWriteConfig =
    HudiWriteConfigBuilder.build(metaClient, tableProps, schema)

  private val recordKeyField = tableProps.getOrElse("hoodie.datasource.write.recordkey.field", "")

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new HudiRowLevelWriterFactory(instantTime, schema, recordKeyField)

  /**
   * Driver-side commit. For every partition touched by this write:
   *   1. Read its keys as they exist right now (pre-commit) via HudiScanBuilder — the same
   *      scan code ordinary reads use, so this reflects exactly what's on the path/timeline.
   *   2. Subtract the keys this write actually produced (the survivors Spark sent).
   *   3. Whatever remains was deleted by this operation.
   *
   * Then: one upsert() for the survivors/updates, one delete() for the computed deletions,
   * both under the same instant, committed together in a single timeline entry.
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val perTask = messages.collect { case m: HudiRowLevelTaskMessage => m }

    val survivorRecords: Seq[HoodieRecord[_ <: HoodieRecordPayload[_]]] =
      perTask.flatMap(_.records)

    val survivorKeysByPartition: Map[String, Set[String]] =
      perTask
        .flatMap(_.keysSeen)
        .groupBy(_._1)
        .map { case (partitionPath, pairs) => partitionPath -> pairs.map(_._2).toSet }

    val touchedPartitions = survivorKeysByPartition.keys.toSeq

    val engineContext = new HoodieSparkEngineContext(spark.sparkContext)
    val writeClient   = new SparkRDDWriteClient(engineContext, writeConfig)

    try {
      val currentKeysByPartition =
        if (touchedPartitions.isEmpty) Map.empty[String, Set[String]]
        else HudiRowLevelUtils.currentKeysByPartition(spark, metaClient, tableType, tableProps, touchedPartitions)

      val deletedKeys: Seq[HoodieKey] = touchedPartitions.flatMap { partitionPath =>
        val current  = currentKeysByPartition.getOrElse(partitionPath, Set.empty)
        val survived = survivorKeysByPartition.getOrElse(partitionPath, Set.empty)
        (current -- survived).map(key => new HoodieKey(key, partitionPath))
      }

      val survivorStatuses: java.util.List[WriteStatus] =
        if (survivorRecords.isEmpty) ju.Collections.emptyList[WriteStatus]()
        else {
          val recordsRdd = spark.sparkContext.parallelize(survivorRecords)
            .toJavaRDD()
            .asInstanceOf[org.apache.spark.api.java.JavaRDD[HoodieRecord[Nothing]]]
          writeClient.upsert(recordsRdd, instantTime).collect()
        }

      val deleteStatuses: java.util.List[WriteStatus] =
        if (deletedKeys.isEmpty) ju.Collections.emptyList[WriteStatus]()
        else {
          val keysRdd = spark.sparkContext.parallelize(deletedKeys).toJavaRDD()
          writeClient.delete(keysRdd, instantTime).collect()
        }

      val allStatuses = new ju.ArrayList[WriteStatus](survivorStatuses.size() + deleteStatuses.size())
      allStatuses.addAll(survivorStatuses)
      allStatuses.addAll(deleteStatuses)

      val commitActionType =
        if (tableType == HoodieTableType.MERGE_ON_READ) HoodieTimeline.DELTA_COMMIT_ACTION
        else HoodieTimeline.COMMIT_ACTION

      writeClient.commit(
        instantTime,
        spark.sparkContext.parallelize(allStatuses.asScala.toSeq),
        HoodieOption.empty(),
        commitActionType,
        ju.Collections.emptyMap[String, ju.List[String]]()
      )
    } catch {
      case e: Exception =>
        writeClient.rollback(instantTime)
        throw new RuntimeException(s"Row-level commit failed at $instantTime", e)
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

/** Per-task result: buffered survivor records to upsert, plus every (partition, key) this
 * task actually saw — the latter is what lets the driver compute deletedKeys without any
 * dependency on Spark telling us which rows were removed. */
case class HudiRowLevelTaskMessage(
                                    instantTime: String,
                                    records: Seq[HoodieRecord[_ <: HoodieRecordPayload[_]]],
                                    keysSeen: Seq[(String, String)] // (partitionPath, recordKey)
                                  ) extends WriterCommitMessage

class HudiRowLevelWriterFactory(
                                 instantTime: String,
                                 schema: StructType,
                                 recordKeyField: String
                               ) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new HudiRowLevelDataWriter(instantTime, schema, recordKeyField)
}

/**
 * Single DataWriter for MERGE/UPDATE/DELETE on either table type — no COW/MOR branching here,
 * that split lives entirely inside SparkRDDWriteClient. Every row this receives is a surviving
 * row (post-filter/post-update content Spark computed); it's buffered as an upsert candidate
 * and its key is recorded so the driver can diff against pre-write partition contents.
 */
class HudiRowLevelDataWriter(
                              instantTime: String,
                              schema: StructType,
                              recordKeyField: String
                            ) extends DataWriter[InternalRow] {

  private val avroSchema     = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "hudi_record", "hoodie")
  private val avroSerializer = new AvroSerializer(schema, avroSchema, nullable = false)

  private val hasMeta = schema.fieldNames.contains("_hoodie_record_key")

  private val metaRecordKeyIdx = if (hasMeta) Some(schema.fieldIndex("_hoodie_record_key")) else None
  private val metaPartitionIdx = if (hasMeta) Some(schema.fieldIndex("_hoodie_partition_path")) else None
  private val metaOrderingIdx  =
    if (hasMeta && schema.fieldNames.contains("_hoodie_commit_time")) Some(schema.fieldIndex("_hoodie_commit_time"))
    else None

  private val userRecordKeyIdx =
    if (!hasMeta && recordKeyField.nonEmpty && schema.fieldNames.contains(recordKeyField))
      Some(schema.fieldIndex(recordKeyField))
    else None

  private val records  = mutable.ArrayBuffer.empty[HoodieRecord[_ <: HoodieRecordPayload[_]]]
  private val keysSeen = mutable.ArrayBuffer.empty[(String, String)]

  override def write(row: InternalRow): Unit = {
    val (recordKey, partitionPath, orderingVal) = if (hasMeta) {
      val rk  = metaRecordKeyIdx.map(i => row.getString(i)).getOrElse("")
      val pp  = metaPartitionIdx.map(i => row.getString(i)).getOrElse("")
      val ord = metaOrderingIdx.map(i => row.getLong(i)).getOrElse(0L)
      (rk, pp, ord)
    } else {
      val rk = userRecordKeyIdx.map(i => row.get(i, schema(i).dataType).toString)
        .getOrElse(java.util.UUID.randomUUID().toString)
      (rk, "", 0L)
    }

    val avroRecord = avroSerializer.serialize(row).asInstanceOf[GenericRecord]
    val hoodieKey  = new HoodieKey(recordKey, partitionPath)
    val payload    = new OverwriteWithLatestAvroPayload(avroRecord, orderingVal)
    records  += new HoodieAvroRecord[OverwriteWithLatestAvroPayload](hoodieKey, payload)
    keysSeen += (partitionPath -> recordKey)
  }

  override def commit(): WriterCommitMessage = {
    val result = HudiRowLevelTaskMessage(instantTime, records.toSeq, keysSeen.toSeq)
    records.clear(); keysSeen.clear()
    result
  }

  override def abort(): Unit = { records.clear(); keysSeen.clear() }
  override def close(): Unit = { records.clear(); keysSeen.clear() }
}

/**
 * Reuses the existing HudiScanBuilder (the same code path ordinary reads go through) to read
 * "what keys currently exist in these partitions" directly off the path/timeline — no new
 * reading/merging logic, no dependency on Spark's catalog beyond what a plain read already needs.
 */
object HudiRowLevelUtils {

  def currentKeysByPartition(
                              spark: SparkSession,
                              metaClient: HoodieTableMetaClient,
                              tableType: HoodieTableType,
                              tableProps: Map[String, String],
                              partitions: Seq[String]
                            ): Map[String, Set[String]] = {
    if (partitions.isEmpty) return Map.empty

    val metaSchema = StructType(Seq(
      org.apache.spark.sql.types.StructField("_hoodie_record_key", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("_hoodie_partition_path", org.apache.spark.sql.types.StringType)
    ))

    val scanBuilder = new HudiScanBuilder(
      spark, metaClient, tableType,
      schema = metaSchema, schemaMeta = metaSchema,
      options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap[String, String]())
    )
    // Column pruning down to just the two key columns — this is the same
    // SupportsPushDownRequiredColumns path a normal projected read would take.
    scanBuilder.pruneColumns(metaSchema)

    val relation = DataSourceV2Relation.create(
      new MultiCatalogHudiPartitionProbeTable(metaClient, metaSchema, scanBuilder),
      catalog = None, identifier = None
    )

    val df: Dataset[Row] = Dataset.ofRows(spark, relation)
      .filter(org.apache.spark.sql.functions.col("_hoodie_partition_path").isin(partitions: _*))

    df.select("_hoodie_partition_path", "_hoodie_record_key")
      .collect()
      .toSeq
      .groupBy(r => r.getString(0))
      .map { case (partitionPath, rows) => partitionPath -> rows.map(_.getString(1)).toSet }
  }
}

/**
 * Minimal Table wrapper so a raw HudiScanBuilder can be driven through DataSourceV2Relation /
 * Dataset.ofRows without going through the catalog. Read-only, used solely to probe current
 * keys for the deletedKeys diff above.
 */
class MultiCatalogHudiPartitionProbeTable(
                                           metaClient: HoodieTableMetaClient,
                                           readSchema: StructType,
                                           scanBuilder: HudiScanBuilder
                                         ) extends org.apache.spark.sql.connector.catalog.Table
  with org.apache.spark.sql.connector.catalog.SupportsRead {

  override def name(): String = s"hudi_probe(${metaClient.getBasePath})"
  override def schema(): StructType = readSchema
  override def capabilities(): java.util.Set[org.apache.spark.sql.connector.catalog.TableCapability] =
    java.util.EnumSet.of(org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = scanBuilder
}
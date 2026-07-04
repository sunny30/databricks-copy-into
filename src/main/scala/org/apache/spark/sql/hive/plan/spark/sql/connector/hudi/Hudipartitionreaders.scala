package org.apache.spark.sql.hive.plan.spark.sql.connector.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.{HoodieEmptyRecord, HoodieRecord}
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.parquet.hadoop.ParquetRecordReader
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.{util => ju}
import scala.collection.JavaConverters._

// ─── COW Reader Factory ───────────────────────────────────────────────────────

class HudiCOWReaderFactory(
                            readSchema: StructType,
                            broadcastHadoopConf: Broadcast[SerializableConfiguration]
                          ) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case p: HudiCOWPartition =>
        new HudiCOWPartitionReader(p, readSchema, broadcastHadoopConf.value.value)
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass.getName}")
    }
}

// ─── COW Partition Reader ─────────────────────────────────────────────────────

/**
 * COW partition reader — delegates to Spark's non-vectorized ParquetRecordReader.
 *
 * Why non-vectorized (ParquetReadSupport + ParquetRecordReader) vs vectorized:
 *   VectorizedParquetRecordReader.initialize(String, List) exists but its column-pruning
 *   behaviour requires careful schema negotiation with the Parquet footer that is
 *   difficult to reproduce without the full FileFormat stack. The non-vectorized reader
 *   handles schema evolution and missing columns correctly out of the box.
 *
 * Spark 3.5.0:
 *   ParquetReadSupport(convertTz: Option[ZoneId], enableVectorizedReader: Boolean)
 *   ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA — sets the projected schema
 *   ParquetReadSupport.SPARK_ROW_REBASE_MODE_IN_READ — date/timestamp rebase
 */
class HudiCOWPartitionReader(
                              partition: HudiCOWPartition,
                              readSchema: StructType,
                              hadoopConf: Configuration
                            ) extends PartitionReader[InternalRow] {

  private val reader: ParquetRecordReader[InternalRow] = buildReader()
  private var currentRow: InternalRow = _

  private def buildReader(): ParquetRecordReader[InternalRow] = {
    val conf = new Configuration(hadoopConf)

    // Spark 3.5.0: set requested schema on the conf so ParquetReadSupport
    // applies column projection inside the Parquet reader itself
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, readSchema.json)
    conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

    // ParquetReadSupport(convertTz = None) — no timezone conversion for partition readers
    val readSupport = new ParquetReadSupport()
    val parquetReader = new ParquetRecordReader[InternalRow](readSupport)

    val filePath = new Path(partition.path)
    val fileStatus = filePath.getFileSystem(conf).getFileStatus(filePath)
    val split = new FileSplit(filePath, 0L, fileStatus.getLen, Array.empty[String])
    val attemptId = new TaskAttemptID()
    val attemptContext = new TaskAttemptContextImpl(conf, attemptId)

    parquetReader.initialize(split, attemptContext)
    parquetReader
  }

  override def next(): Boolean = {
    if (reader.nextKeyValue()) {
      currentRow = reader.getCurrentValue
      true
    } else false
  }

  override def get(): InternalRow = currentRow
  override def close(): Unit = reader.close()
}

// ─── MOR Reader Factory ───────────────────────────────────────────────────────

class HudiMORReaderFactory(
                            readSchema: StructType,
                            fullSchema: StructType,
                            basePath: String,
                            broadcastHadoopConf: Broadcast[SerializableConfiguration],
                            options: ju.Map[String, String]
                          ) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    partition match {
      case p: HudiMORPartition =>
        new HudiMORPartitionReader(
          partition           = p,
          readSchema          = readSchema,
          fullSchema          = fullSchema,
          tableBasePath       = basePath,
          hadoopConf          = broadcastHadoopConf.value.value,
          options             = options
        )
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass.getName}")
    }
}

// ─── MOR Partition Reader ─────────────────────────────────────────────────────

/**
 * MOR partition reader.
 *
 * Algorithm:
 *   1. Build HoodieMergedLogRecordScanner over all log files for this file group.
 *      Scanner builds an in-memory map: recordKey → latest HoodieRecord (Avro payload).
 *   2. Read base Parquet row by row (via HudiCOWPartitionReader internally).
 *   3. For each base row:
 *        - extract _hoodie_record_key
 *        - if key in log scanner DELETE set  → skip (deleted)
 *        - if key in log scanner record map  → emit merged (log wins)
 *        - else                              → emit base row projected to readSchema
 *   4. After base exhausted: emit net-new inserts from log (not present in base).
 *
 * Hudi 1.0.1 API changes:
 *   HoodieMergedLogRecordScanner.newBuilder()
 *     .withStorage(HoodieStorage)          ← replaces .withFileSystem(FileSystem)
 *     .withBasePath(HoodieStoragePath)     ← replaces hadoop Path
 *     .withLogFilePaths(List<String>)      ← String paths, not HoodieStoragePath
 *     .build()
 *   scanner.getRecords() → Map<String, HoodieRecord<IndexedRecord>>
 *   scanner.getDeletedKeys() → Set<String>
 *
 * Spark 3.5.0 AvroDeserializer (from spark-avro module):
 *   new AvroDeserializer(avroSchema, sparkSchema, datetimeRebaseMode)
 *   .deserialize(avroRecord) → Option[Any]  — cast result to InternalRow
 */
class HudiMORPartitionReader(
                              partition: HudiMORPartition,
                              readSchema: StructType,
                              fullSchema: StructType,
                              tableBasePath: String,
                              hadoopConf: Configuration,
                              options: ju.Map[String, String]
                            ) extends PartitionReader[InternalRow] {

  // ── Avro schema setup ─────────────────────────────────────────────────────

  // Writer schema: full table schema (including meta fields) used by log records
  private val writerAvroSchema: Schema =
    AvroConversionUtils.convertStructTypeToAvroSchema(fullSchema, "hudi_record", "hoodie")

  // Reader schema: projected schema requested by the scan
  private val readerAvroSchema: Schema =
    AvroConversionUtils.convertStructTypeToAvroSchema(readSchema, "hudi_record", "hoodie")

  /**
   * Spark 3.5.0 AvroDeserializer:
   *   org.apache.spark.sql.avro.AvroDeserializer
   *   Constructor: (rootAvroType: Schema, rootCatalystType: DataType, datetimeRebaseMode: String)
   *   deserialize(data: Any): Option[Any]
   */
  private val avroDeserializer = new AvroDeserializer(
    readerAvroSchema,
    readSchema,
    "CORRECTED"   // datetimeRebaseMode — matches Spark 3.5 default
  )

  // ── Log scanner ───────────────────────────────────────────────────────────

  /**
   * Hudi 1.0.1:
   *   HoodieHadoopStorage wraps FileSystem with HoodieStorage abstraction
   *   HoodieStoragePath wraps path strings
   *   Scanner builder uses .withStorage() and .withBasePath(HoodieStoragePath)
   */
  private val logScanner: Option[HoodieMergedLogRecordScanner] =
    if (partition.logPaths.isEmpty) None
    else Some(buildLogScanner())

  // Eagerly materialise records from scanner into a Map (scanner.scan() is called in builder)
  // In Hudi 1.0.1: getRecords() returns Map<String, HoodieRecord<IndexedRecord>>
  //
  // NOTE: previously this did `hudiRecord.getData.asInstanceOf[GenericRecord]` unconditionally,
  // which throws ClassCastException on a delete tombstone entry (a HoodieEmptyRecord's getData()
  // is not a GenericRecord). Guarded with collect{} so tombstones are excluded here and picked
  // up correctly below via deletedKeys instead of crashing the whole read.
  private val logRecordMap: Map[String, GenericRecord] = logScanner match {
    case None    => Map.empty
    case Some(s) =>
      s.getRecords.asScala.collect {
        case (key, hudiRecord)
          if !hudiRecord.isInstanceOf[HoodieEmptyRecord[_]] && hudiRecord.getData.isInstanceOf[GenericRecord] =>
          key -> hudiRecord.getData.asInstanceOf[GenericRecord]
      }.toMap
  }

  // Keys explicitly deleted via DELETE_BLOCK in the log.
  //
  // Two things were wrong with the original: (1) it called `rec.getData.isInstanceOf
  // [HoodieEmptyRecord[_]]` — but HoodieEmptyRecord is a HoodieRecord *subtype* representing the
  // tombstone itself, never something getData() (the payload accessor) returns, so that check
  // was always false. (2) my own first attempt at fixing this called scanner.getDeletedKeys(),
  // which doesn't exist on HoodieMergedLogRecordScanner in this Hudi version — my mistake, that
  // was an unverified guess and the compiler correctly rejected it. Back to getRecords(), but
  // testing the HoodieRecord wrapper itself for HoodieEmptyRecord, not its payload.
  private val deletedKeys: Set[String] = logScanner match {
    case None    => Set.empty
    case Some(s) => s.getRecords.asScala.collect {
      case (key, hudiRecord) if hudiRecord.isInstanceOf[HoodieEmptyRecord[_]] => key
    }.toSet
  }

  // ── Base file reader ──────────────────────────────────────────────────────

  private val baseReader: Option[HudiCOWPartitionReader] =
    partition.basePath.map { path =>
      // Read full schema from base so we can extract _hoodie_record_key
      new HudiCOWPartitionReader(
        HudiCOWPartition(path, partition.fileSize, InternalRow.empty),
        fullSchema,
        hadoopConf
      )
    }

  private val seenKeys = scala.collection.mutable.HashSet.empty[String]

  // ── Read phase state ──────────────────────────────────────────────────────

  private sealed trait Phase
  private case object BasePhase    extends Phase
  private case object LogOnlyPhase extends Phase
  private case object Done         extends Phase

  private var phase: Phase = if (baseReader.isDefined) BasePhase else LogOnlyPhase

  private lazy val netNewLogIterator: Iterator[(String, GenericRecord)] =
    logRecordMap.iterator.filterNot { case (key, _) => seenKeys.contains(key) || deletedKeys.contains(key) }

  private var currentRow: InternalRow = _

  // ── PartitionReader interface ─────────────────────────────────────────────

  override def next(): Boolean = phase match {
    case BasePhase    => advanceBase()
    case LogOnlyPhase => advanceLogOnly()
    case Done         => false
  }

  override def get(): InternalRow  = currentRow
  override def close(): Unit = {
    baseReader.foreach(_.close())
    logScanner.foreach(_.close())
  }

  // ── Phase: base + log merge ───────────────────────────────────────────────

  private def advanceBase(): Boolean = {
    val reader = baseReader.get
    while (reader.next()) {
      val baseRow   = reader.get()
      val recordKey = baseRow.getString(fullSchema.fieldIndex("_hoodie_record_key"))
      seenKeys.add(recordKey)

      if (deletedKeys.contains(recordKey)) {
        // deleted — skip
      } else if (logRecordMap.contains(recordKey)) {
        // updated in log — emit log version projected to readSchema
        currentRow = avroToRow(logRecordMap(recordKey))
        return true
      } else {
        // not in log — emit base row projected to readSchema
        currentRow = projectBaseRow(baseRow)
        return true
      }
    }
    // Base exhausted — move to net-new log inserts
    phase = LogOnlyPhase
    advanceLogOnly()
  }

  // ── Phase: net-new log inserts (keys not present in base) ─────────────────

  private def advanceLogOnly(): Boolean = {
    if (!netNewLogIterator.hasNext) {
      phase = Done
      return false
    }
    val (_, logRecord) = netNewLogIterator.next()
    currentRow = avroToRow(logRecord)
    true
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def buildLogScanner(): HoodieMergedLogRecordScanner = {
    val maxMemory  = options.getOrDefault("hoodie.memory.merge.max.size", (100 * 1024 * 1024).toString).toLong
    val bufferSize = options.getOrDefault("hoodie.logfile.data.block.max.size", (256 * 1024 * 1024).toString).toInt
    val spillPath  = options.getOrDefault("hoodie.spillable.map.path", "/tmp/hoodie-mor-spill")

    // Hudi 1.0.1 storage abstraction
    val storagePath = new StoragePath(tableBasePath)
    val storage     = new HoodieHadoopStorage(storagePath, new HadoopStorageConfiguration(hadoopConf))

    HoodieMergedLogRecordScanner.newBuilder()
      .withStorage(storage)                              // Hudi 1.0.1: replaces withFileSystem
      .withBasePath(new StoragePath(tableBasePath)) // Hudi 1.0.1: HoodieStoragePath
      .withLogFilePaths(partition.logPaths.toList.asJava) // List<String>
      .withReaderSchema(writerAvroSchema)
      .withLatestInstantTime(partition.latestInstantTime)
      .withMaxMemorySizeInBytes(maxMemory)
      .withReverseReader(false)
      .withBufferSize(bufferSize)
      .withSpillableMapBasePath(spillPath)
      .withDiskMapType(
        org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue()
      )
      .withBitCaskDiskMapCompressionEnabled(false)
      .build()
  }

  /**
   * Avro GenericRecord → InternalRow via Spark 3.5 AvroDeserializer.
   *
   * HoodieAvroUtils.rewriteRecord projects/reorders the log record to match
   * the readerAvroSchema (handles schema evolution: new fields get defaults).
   *
   * AvroDeserializer.deserialize returns Option[Any] in Spark 3.5.
   * None indicates a filtered/null record — we emit empty row in that case.
   */
  private def avroToRow(record: GenericRecord): InternalRow = {
    val rewritten = HoodieAvroUtils.rewriteRecord(record, readerAvroSchema)
    avroDeserializer.deserialize(rewritten) match {
      case Some(row) => row.asInstanceOf[InternalRow]
      case None      => InternalRow.empty
    }
  }

  /**
   * Project a full-schema InternalRow (from base Parquet) to readSchema.
   * Maps field positions: for each field in readSchema, look up its index in fullSchema.
   */
  private def projectBaseRow(row: InternalRow): InternalRow = {
    val indices = readSchema.fields.map(f => fullSchema.fieldIndex(f.name))
    val values  = indices.zipWithIndex.map { case (srcIdx, _) =>
      row.get(srcIdx, fullSchema(srcIdx).dataType)
    }
    InternalRow.fromSeq(values)
  }
}
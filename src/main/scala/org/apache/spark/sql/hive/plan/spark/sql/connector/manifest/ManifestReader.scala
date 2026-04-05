package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

// ═════════════════════════════════════════════════════════════════════════════
//  ManifestState + CommittedManifest  — sealed ADT returned by ManifestReader
// ═════════════════════════════════════════════════════════════════════════════

sealed trait ManifestState

object ManifestState {
  /** Clean committed write: _committed_<tid> present, no orphaned _started_. */
  case class Committed(manifest: CommittedManifest) extends ManifestState

  /** Write in progress or failed: _started_<tid> without _committed_<tid>. */
  case class Incomplete(startedTid: String) extends ManifestState

  /** No manifest files at all — legacy data or first write not yet committed. */
  case object NoManifest extends ManifestState
}

/**
 * Parsed content of one _committed_<tid> file.
 * addedFiles contains bare filenames only — NOT full paths.
 */
case class CommittedManifest(
                              tid:        String,
                              addedFiles: Seq[String],
                              removedFiles: Seq[String]
                            )

// ═════════════════════════════════════════════════════════════════════════════
//  ManifestReader
//
//  Manifest file location rule (matching ManifestCommitProtocol):
//
//    PARTITIONED table:
//      Each partition dir contains its own _committed_<tid> and _started_<tid>
//      → check the PARTITION directory
//      e.g. s3://bucket/table/year=2024/month=01/_committed_7628...
//
//    UNPARTITIONED table:
//      Manifest lives at the table root
//      → check the ROOT directory
//      e.g. s3://bucket/table/_committed_7628...
//
//  Both locations are checked identically by readManifestState(dir, fs).
//  The caller decides which dir to pass (partition dir or root).
// ═════════════════════════════════════════════════════════════════════════════

object ManifestReader extends Logging {

  /**
   * Reads the manifest state of ONE directory — either a partition dir or root.
   *
   * Single listStatus() call per directory.
   * Returns the LATEST clean _committed_ if multiple exist (e.g. after several writes).
   *
   * Algorithm:
   *   1. List all files in dir (single FS call)
   *   2. Collect all _committed_<tid> by tid
   *   3. Collect all _started_<tid> tids
   *   4. incompleteTids = startedTids - committedTids.keySet
   *   5. If any incompleteTid → Incomplete
   *   6. If no committed files → NoManifest
   *   7. Else → parse and return latest _committed_ → Committed
   */
  def readManifestState(dir: Path, fs: FileSystem): ManifestState = {
    val statuses = try {
      fs.listStatus(dir)
    } catch {
      case e: Exception =>
        logWarning(s"ManifestReader: listStatus failed at $dir: ${e.getMessage}")
        return ManifestState.NoManifest
    }

    // Partition _committed_ by their tid
    val committedByTid: Map[String, org.apache.hadoop.fs.FileStatus] = statuses
      .filter(_.getPath.getName.startsWith("_committed_"))
      .map(s => s.getPath.getName.stripPrefix("_committed_") -> s)
      .toMap

    // Collect _started_ tids
    val startedTids: Set[String] = statuses
      .filter(_.getPath.getName.startsWith("_started_"))
      .map(_.getPath.getName.stripPrefix("_started_"))
      .toSet

    // orphaned = _started_ without matching _committed_ = incomplete/failed
    val incompleteTids: Set[String] = startedTids -- committedByTid.keySet

    if (incompleteTids.nonEmpty) {
      return ManifestState.Incomplete(incompleteTids.head)
    }

    if (committedByTid.isEmpty) {
      return ManifestState.NoManifest
    }

    // Pick latest _committed_ by file modification time
    val latestStatus = committedByTid.values
      .maxBy(_.getModificationTime)

    val tid = latestStatus.getPath.getName.stripPrefix("_committed_")

    try {
      ManifestState.Committed(parseManifest(tid, latestStatus.getPath, fs))
    } catch {
      case e: Exception =>
        logWarning(
          s"ManifestReader: failed to parse ${latestStatus.getPath}: ${e.getMessage}. " +
            s"Treating as NoManifest.")
        ManifestState.NoManifest
    }
  }

  /**
   * Returns the previous clean _committed_ manifest in the given directory,
   * ignoring any tids that also have a _started_ (i.e. in-progress writes).
   *
   * Used by ManifestAwareFileIndex when an incomplete write is detected and
   * failOnIncompleteWrite=false — we fall back to serving old committed data.
   */
  def previousCommittedManifest(dir: Path, fs: FileSystem): Option[CommittedManifest] = {
    val statuses = try { fs.listStatus(dir) }
    catch { case _: Exception => return None }

    val startedTids: Set[String] = statuses
      .filter(_.getPath.getName.startsWith("_started_"))
      .map(_.getPath.getName.stripPrefix("_started_"))
      .toSet

    // All _committed_ files whose tid does NOT have a _started_ = prior clean writes
    val cleanCommitted = statuses
      .filter(s => s.getPath.getName.startsWith("_committed_") &&
        !startedTids.contains(s.getPath.getName.stripPrefix("_committed_")))
      .sortBy(_.getModificationTime)
      .lastOption

    cleanCommitted.flatMap { status =>
      val tid = status.getPath.getName.stripPrefix("_committed_")
      try { Some(parseManifest(tid, status.getPath, fs)) }
      catch { case _: Exception => None }
    }
  }

  /** True if a directory has _started_ without matching _committed_. */
  def hasIncompleteWrite(dir: Path, fs: FileSystem): Boolean =
    readManifestState(dir, fs).isInstanceOf[ManifestState.Incomplete]

  // ── Parser ─────────────────────────────────────────────────────────────────

  /**
   * Parses _committed_<tid> JSON.
   * Expected format written by ManifestCommitProtocol:
   *   {"added":["part-00000-tid-<tid>-...-c000.snappy.parquet"],"removed":[]}
   *
   * Hand-rolled — no Jackson dependency needed. Safe because
   * ManifestCommitProtocol writes simple flat JSON.
   */
  def parseManifest(tid: String, path: Path, fs: FileSystem): CommittedManifest = {
    val in = fs.open(path)
    val content = try {
      val buf = new Array[Byte](Math.min(in.available(), 10 * 1024 * 1024))
      in.readFully(buf)
      new String(buf, "UTF-8").trim
    } finally {
      in.close()
    }

    CommittedManifest(
      tid          = tid,
      addedFiles   = extractJsonStringArray(content, "added"),
      removedFiles = extractJsonStringArray(content, "removed")
    )
  }

  /**
   * Extracts a JSON string array for a given key from flat JSON.
   * Handles: {"key":["val1","val2"],...}
   * Does NOT handle nested objects or escaped characters in filenames.
   */
  private def extractJsonStringArray(json: String, key: String): Seq[String] = {
    val pattern = s""""$key"\\s*:\\s*\\[([^\\]]*)\\]""".r
    pattern.findFirstMatchIn(json) match {
      case None    => Seq.empty
      case Some(m) =>
        val inner = m.group(1).trim
        if (inner.isEmpty) Seq.empty
        else inner.split(",")
          .map(_.trim.stripPrefix("\"").stripSuffix("\""))
          .filter(_.nonEmpty)
          .toSeq
    }
  }
}

// ═════════════════════════════════════════════════════════════════════════════
//  ManifestAwareFileIndex
//
//  Extends InMemoryFileIndex with:
//    1. Manifest-aware file listing per partition directory
//    2. Partition schema override (applied AFTER inferPartitioning())
//
//  Manifest location logic (matches ManifestCommitProtocol):
//    Partitioned table  → manifest lives inside each partition folder
//    Unpartitioned      → manifest lives at the root
//
//  listFiles() intercepts AFTER super returns PartitionDirectory results.
//  For each PartitionDirectory, the directory is either:
//    - a partition leaf dir (year=2024/month=01) for partitioned tables
//    - the root table dir for unpartitioned tables
//  In both cases we call ManifestReader.readManifestState(that dir, fs).
// ═════════════════════════════════════════════════════════════════════════════

/**
 * @param overridePartitionSchema  Optional. When provided, replaces inferred
 *   partition column types after inferPartitioning() runs.
 *   Column count must match inferred depth. Column names from the override
 *   replace inferred names only if they differ (with a warning).
 *   Typical use: supply catalog partition schema to prevent Spark from
 *   inferring IntegerType from "year=2024" and causing partition pruning
 *   mismatches with StringType predicates.
 */
class ManifestAwareFileIndex(
                              sparkSession: SparkSession,
                              rootPathsSpecified: Seq[Path],
                              parameters: Map[String, String],
                              userSpecifiedSchema: Option[StructType],
                              fileStatusCache: FileStatusCache = NoopCache,
                              userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
                              override val metadataOpsTimeNs: Option[Long] = None,
                              val overridePartitionSchema: Option[StructType] = None
                            ) extends InMemoryFileIndex(
  sparkSession,
  rootPathsSpecified,
  parameters,
  userSpecifiedSchema,
  fileStatusCache,
  userSpecifiedPartitionSpec,
  metadataOpsTimeNs)
  with Logging {

  // ── Config ─────────────────────────────────────────────────────────────────
  private val failOnIncomplete: Boolean =
    sparkSession.conf.get("spark.sql.manifest.failOnIncompleteWrite", "true").toBoolean

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  // ═══════════════════════════════════════════════════════════════════════════
  //  PARTITION SCHEMA OVERRIDE
  //
  //  InMemoryFileIndex.partitionSpec() (exact code from source):
  //    if (cachedPartitionSpec == null) {
  //      if (userSpecifiedPartitionSpec.isDefined)
  //        cachedPartitionSpec = userSpecifiedPartitionSpec.get
  //      else
  //        cachedPartitionSpec = inferPartitioning()
  //    }
  //    cachedPartitionSpec
  //
  //  We intercept the result of super.partitionSpec() and:
  //    - If overridePartitionSchema is None   → return super result unchanged
  //    - If overridePartitionSchema is Some   → rewrite column types + recast
  //                                             partition InternalRow values
  //
  //  We do NOT override inferPartitioning() because we need the partition
  //  paths (InternalRow values) that inferPartitioning() discovers from dirs.
  // ═══════════════════════════════════════════════════════════════════════════

  override def partitionSpec(): PartitionSpec = {
    val baseSpec = super.partitionSpec()

    overridePartitionSchema match {
      case None => baseSpec

      case Some(overrideSchema) =>
        applyPartitionSchemaOverride(baseSpec, overrideSchema)
    }
  }

  private def applyPartitionSchemaOverride(
                                            base:     PartitionSpec,
                                            override_ : StructType): PartitionSpec = {

    val inferredCols = base.partitionColumns

    // ── Guard: unpartitioned table ─────────────────────────────────────────
    if (inferredCols.fields.isEmpty) {
      if (override_.fields.nonEmpty)
        logWarning("ManifestAwareFileIndex: overridePartitionSchema provided but " +
          "table is unpartitioned. Override ignored.")
      return base
    }

    // ── Guard: column count mismatch ───────────────────────────────────────
    if (override_.fields.length != inferredCols.fields.length) {
      logWarning(
        s"ManifestAwareFileIndex: overridePartitionSchema has ${override_.fields.length} " +
          s"columns but inferred schema has ${inferredCols.fields.length} columns. " +
          s"Override ignored.")
      return base
    }

    // ── Build new StructType: merge names + replace types ──────────────────
    val newFields: Array[StructField] = inferredCols.fields.zipWithIndex.map {
      case (inferredField, i) =>
        val overrideField = override_.fields(i)
        val finalName =
          if (overrideField.name.nonEmpty && overrideField.name != inferredField.name) {
            logWarning(
              s"ManifestAwareFileIndex: partition column[$i] name mismatch: " +
                s"inferred='${inferredField.name}' override='${overrideField.name}'. " +
                s"Using override name.")
            overrideField.name
          } else if (overrideField.name.nonEmpty) {
            overrideField.name
          } else {
            inferredField.name
          }
        StructField(finalName, overrideField.dataType, overrideField.nullable)
    }
    val newColumns = StructType(newFields)

    // ── Recast each partition's InternalRow values to new types ────────────
    val newPartitions = base.partitions.map { partPath =>
      val oldRow = partPath.values
      val newValues = new Array[Any](newColumns.fields.length)

      newColumns.fields.zipWithIndex.foreach { case (newField, i) =>
        val inferredField = inferredCols.fields(i)
        val rawValue      = oldRow.get(i, inferredField.dataType)
        newValues(i) = castPartitionValue(
          rawValue, inferredField.dataType, newField.dataType, partPath.path.toString)
      }

      PartitionPath(InternalRow.fromSeq(newValues), partPath.path)
    }

    logDebug(
      s"ManifestAwareFileIndex: partition schema override applied. " +
        s"Inferred: ${inferredCols.simpleString} → Override: ${newColumns.simpleString}")

    PartitionSpec(newColumns, newPartitions)
  }

  // ── Type casting for partition column values ────────────────────────────────
  // Partition InternalRow values are typed Java objects (Int, Long, UTF8String, etc.)
  // We cast from inferred type to the override type.

  private def castPartitionValue(
                                  value:    Any,
                                  fromType: DataType,
                                  toType:   DataType,
                                  path:     String): Any = {

    if (fromType == toType || value == null) return value

    try {
      (fromType, toType) match {
        // Numeric widening
        case (IntegerType, LongType)    => value.asInstanceOf[Int].toLong
        case (IntegerType, DoubleType)  => value.asInstanceOf[Int].toDouble
        case (IntegerType, FloatType)   => value.asInstanceOf[Int].toFloat
        case (IntegerType, ShortType)   => value.asInstanceOf[Int].toShort
        case (LongType, IntegerType)    => value.asInstanceOf[Long].toInt
        case (LongType, DoubleType)     => value.asInstanceOf[Long].toDouble
        case (DoubleType, FloatType)    => value.asInstanceOf[Double].toFloat
        case (FloatType, DoubleType)    => value.asInstanceOf[Float].toDouble

        // Any → String (Spark stores strings as UTF8String in InternalRow)
        case (_, StringType) =>
          UTF8String.fromString(value.toString)

        // String → numeric (partition values stored as UTF8String after parsing)
        case (StringType, IntegerType) =>
          value.asInstanceOf[UTF8String].toString.toInt
        case (StringType, LongType) =>
          value.asInstanceOf[UTF8String].toString.toLong
        case (StringType, DoubleType) =>
          value.asInstanceOf[UTF8String].toString.toDouble
        case (StringType, FloatType) =>
          value.asInstanceOf[UTF8String].toString.toFloat
        case (StringType, BooleanType) =>
          value.asInstanceOf[UTF8String].toString.toBoolean

        // Date/timestamp: already stored as epoch days / epoch micros
        case (IntegerType, DateType)    => value
        case (LongType, TimestampType)  => value

        // General fallback: use Spark's Cast expression
        case _ =>
          val result = Cast(Literal.create(value, fromType), toType).eval(InternalRow.empty)
          if (result == null) {
            logWarning(s"ManifestAwareFileIndex: cast $value $fromType→$toType " +
              s"returned null at $path. Keeping inferred value.")
            value
          } else result
      }
    } catch {
      case e: Exception =>
        logWarning(s"ManifestAwareFileIndex: failed to cast '$value' " +
          s"$fromType→$toType at $path: ${e.getMessage}. Keeping inferred value.")
        value
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  MANIFEST-AWARE FILE LISTING
  //
  //  super.listFiles() (PartitioningAwareFileIndex):
  //    For PARTITIONED tables:
  //      prunePartitions() → Seq[PartitionPath(values, path)]
  //      for each: leafDirToChildrenFiles.get(path) → files in that dir
  //      returns Seq[PartitionDirectory(values, files)]
  //      ↑ Here 'path' IS the partition leaf dir (year=2024/month=01)
  //        so we check manifest in THAT dir  ← correct per ManifestCommitProtocol
  //
  //    For UNPARTITIONED tables:
  //      PartitionDirectory(InternalRow.empty, allFiles())
  //      ↑ files are from rootPaths
  //        we check manifest at rootPath  ← correct per ManifestCommitProtocol
  //
  //  After super.listFiles() returns, we iterate each PartitionDirectory and:
  //    - Determine the directory to check (partDir or rootPath)
  //    - Run ManifestReader.readManifestState(dir, fs)
  //    - Filter / replace the file array accordingly
  // ═══════════════════════════════════════════════════════════════════════════

  override def listFiles(
                          partitionFilters: Seq[Expression],
                          dataFilters:      Seq[Expression]): Seq[PartitionDirectory] = {

    val rawPartitions = super.listFiles(partitionFilters, dataFilters)

    rawPartitions.map { partDir =>
      // ── Determine which directory holds this partition's manifest ─────────
      // For partitioned tables: files are inside year=2024/month=01/ → use parent
      // For unpartitioned tables: files are directly in rootPath
      // Both cases: the directory is the parent of any file in the partition
      val dirPath: Path = determineManifestDir(partDir)

      if (dirPath == null) {
        // Empty partition with no files — nothing to check
        partDir
      } else {
        val fs = dirPath.getFileSystem(hadoopConf)
        applyManifestFilter(partDir, dirPath, fs)
      }
    }
  }

  /**
   * Determines the directory to check for manifest files for a given PartitionDirectory.
   *
   * For a non-empty PartitionDirectory the directory is the parent of the first file.
   * This is always the partition leaf directory (year=2024/month=01) for partitioned
   * tables, or the root directory for unpartitioned tables — which exactly matches
   * where ManifestCommitProtocol writes _committed_/_started_.
   *
   * Returns null if the partition has no files.
   */
  private def determineManifestDir(partDir: PartitionDirectory): Path = {
    partDir.files.headOption match {
      case Some(status) => status.getPath.getParent
      case None         =>
        // No files in partition — for unpartitioned this shouldn't happen.
        // For partitioned with empty partition, we can check rootPath.
        // Return null to skip manifest check (nothing to filter anyway).
        null
    }
  }

  /**
   * Applies manifest filtering to a single PartitionDirectory.
   *
   * Manifests checked in the PARTITION directory (or root for unpartitioned).
   * This is correct because ManifestCommitProtocol writes:
   *   - partitioned:   year=2024/month=01/_committed_<tid>
   *   - unpartitioned: <rootPath>/_committed_<tid>
   */
  private def applyManifestFilter(
                                   partDir: PartitionDirectory,
                                   dir:     Path,
                                   fs:      FileSystem): PartitionDirectory = {

    ManifestReader.readManifestState(dir, fs) match {

      // ── Case 1: Clean committed write ───────────────────────────────────
      // Filter files to only those listed in the manifest.
      // Protects against reading files from a concurrent write or a
      // partially cleaned-up failed write.
      case ManifestState.Committed(manifest) =>
        val committedNames: Set[String] = manifest.addedFiles.toSet
        val filtered = partDir.files.filter { f =>
          committedNames.contains(f.getPath.getName)
        }
        logDebug(
          s"ManifestAwareFileIndex[$dir]: manifest=${committedNames.size} " +
            s"raw=${partDir.files.length} after_filter=${filtered.length}")
        partDir.copy(files = filtered)

      // ── Case 2a: Incomplete write — fail fast ────────────────────────────
      case ManifestState.Incomplete(tid) if failOnIncomplete =>
        throw new IllegalStateException(
          s"ManifestAwareFileIndex: incomplete write detected at $dir " +
            s"(_started_$tid exists without matching _committed_$tid). " +
            s"The previous write likely failed. " +
            s"To fall back to old data, set: " +
            s"spark.sql.manifest.failOnIncompleteWrite=false")

      // ── Case 2b: Incomplete write — fall back to previous manifest ───────
      // A write started but didn't commit. Serve the previously committed data.
      case ManifestState.Incomplete(tid) =>
        logWarning(
          s"ManifestAwareFileIndex[$dir]: incomplete write (tid=$tid). " +
            s"Falling back to previous committed manifest.")

        ManifestReader.previousCommittedManifest(dir, fs) match {

          case Some(prevManifest) =>
            val prevNames: Set[String] = prevManifest.addedFiles.toSet
            val prevFiles = partDir.files.filter { f =>
              prevNames.contains(f.getPath.getName)
            }
            logInfo(
              s"ManifestAwareFileIndex[$dir]: serving ${prevFiles.length} files " +
                s"from previous manifest (tid=${prevManifest.tid})")
            partDir.copy(files = prevFiles)

          case None =>
            // No previous manifest exists — this is the very first write and it failed.
            // Return empty to avoid reading partial files.
            logWarning(
              s"ManifestAwareFileIndex[$dir]: no previous manifest found. " +
                s"Returning empty partition to avoid reading partial write.")
            partDir.copy(files = Array.empty[FileStatusWithMetadata].toSeq)
        }

      // ── Case 3: No manifest — legacy data ───────────────────────────────
      // Written before ManifestCommitProtocol was deployed.
      // Fall through to raw directory listing unchanged.
      case ManifestState.NoManifest =>
        logDebug(s"ManifestAwareFileIndex[$dir]: no manifest — returning raw listing.")
        partDir
    }
  }

  override def refresh(): Unit = {
    super.refresh()
    // ManifestReader is stateless — reads from FS each call, no extra cache to clear.
    logDebug(s"ManifestAwareFileIndex: refreshed rootPaths=$rootPaths")
  }
}
//```
//
//---
//
//## What each class does and why
//
//### `ManifestReader` — stateless, pure FS reads
//```
//readManifestState(dir, fs):
//  dir = year=2024/month=01/   (partition leaf dir — passed by listFiles)
//OR
//dir = s3://bucket/table/    (root — for unpartitioned tables)
//
//  lists files in dir → classifies into Committed / Incomplete / NoManifest
//    returns the LATEST _committed_ by modification time
//(handles multiple _committed_ files from successive writes gracefully)
//```
//
//### `ManifestAwareFileIndex` — two concerns, cleanly separated
//  ```
//partitionSpec() override:
//  calls super.partitionSpec()         ← runs inferPartitioning() to discover paths
//    then applies overridePartitionSchema ← replaces column types + recasts InternalRow
//    result: correct partition paths with correct types
//
//    listFiles() override:
//  calls super.listFiles()             ← handles partition pruning correctly
//  for each PartitionDirectory returned:
//    determines manifest dir from first file's parent  ← partition dir OR root
//    calls ManifestReader.readManifestState(dir, fs)
//  applies Committed / Incomplete / NoManifest filtering
//  ```
//
//  ### Manifest directory rule — matches `ManifestCommitProtocol` exactly
//  ```
//  ManifestCommitProtocol writes:
//    partitioned:   newTaskTempFile(dir=Some("year=2024/month=01"))
//  → touchFile at  outputPath/year=2024/month=01/_started_<tid>
//  → writeJson at  outputPath/year=2024/month=01/_committed_<tid>
//
//  unpartitioned: newTaskTempFile(dir=None)
//  → touchFile at  outputPath/_started_<tid>
//    → writeJson at  outputPath/_committed_<tid>
//
//  ManifestAwareFileIndex reads:
//    partitioned:   partDir.files.head.getPath.getParent
//  = outputPath/year=2024/month=01   ← same dir ✅
//
//  unpartitioned: partDir.files.head.getPath.getParent
//  = outputPath                       ← same dir ✅
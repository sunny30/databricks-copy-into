package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.{TaskCommitMessage}
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import java.time.Instant
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
//  Top-level types — defined outside the class so pattern matching works
//  correctly after Java serialization/deserialization on the driver.
// ─────────────────────────────────────────────────────────────────────────────

/** Carries both super's Tuple2 and our partition→files map in one TaskCommitMessage. */
final case class CombinedCommitPayloadV4(
                                        superObj:       Any,
                                        partitionFiles: java.util.HashMap[String, java.util.List[String]]
                                      )

/** Parsed content of a _started_<tid> file. */
final case class StartedManifest(
                                  jobId:         String,
                                  partitionPath: String,
                                  startedAt:     String,
                                  outputPath:    String,
                                  dynamic:       Boolean,
                                  pendingFiles:  Seq[String]   // data files written by tasks, not yet job-committed
                                )

/** Parsed content of a _committed_<tid> file. */
final case class CommittedManifest(
                                    tid:          String,
                                    addedFiles:   Seq[String],
                                    removedFiles: Seq[String]
                                  )

// ─────────────────────────────────────────────────────────────────────────────
//  ManifestCommitProtocol
//
//  DBIO-compatible manifest protocol for open-source Spark 3.5.
//
//  File layout:
//    Partitioned:   table/p=1/_started_<tid>    table/p=1/_committed_<tid>
//    Unpartitioned: table/_started_<tid>         table/_committed_<tid>
//    Root always:   table/_started_<tid>         table/_committed_<tid>
//
//  _started_ content  → job metadata + pendingFiles written by tasks
//  _committed_ content → added[] and removed[] file lists
//
//  Recovery (next setupJob):
//    PATH A  _started_ only            → delete pendingFiles (uncommitted garbage)
//    PATH B  _started_ + _committed_   → complete deferred old-file deletions
//
//  Optimized deletes → parallel thread pool, configurable parallelism
// ─────────────────────────────────────────────────────────────────────────────

class ManifestFileCommitProtocolV4(
                              jobId: String,
                              outputPath: String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ═══════════════════════════════════════════════════════════════════════════
  //  SERIALIZATION CONTRACT
  //
  //  Committer is Java-serialized on DRIVER → sent to EXECUTORS.
  //  @transient fields are NULL after deserialization.
  //
  //  DRIVER-side   → initialize in setupJob()
  //  EXECUTOR-side → initialize in setupTask()   ← NEVER inline
  // ═══════════════════════════════════════════════════════════════════════════

  // ── DRIVER-SIDE ────────────────────────────────────────────────────────────
  @transient private var outputDir: Path   = _
  @transient private var jobFs: FileSystem = _

  // partDir(qualified) → old data filenames captured before the write.
  // Populated by collectOldFilesRecursively (called from deleteWithJob).
  // Used in step 6 to delete old files and in _committed_.removed[].
  @transient private val pendingDeleteByDir =
  new java.util.LinkedHashMap[String, java.util.List[String]]()

  // Delete parallelism — configurable via Spark conf
  private val deleteParallelism: Int = 8   // overridden in setupJob from conf

  // ── EXECUTOR-SIDE — MUST be initialized in setupTask(), never inline ───────
  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String, java.util.concurrent.CopyOnWriteArrayList[String]] = _

  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ═══════════════════════════════════════════════════════════════════════════
  //  HELPERS — file classification
  // ═══════════════════════════════════════════════════════════════════════════

  /** A data file: starts with neither _ nor . (excludes manifests, crc, etc.) */
  @inline private def isDataFile(name: String): Boolean =
    name.nonEmpty && !name.startsWith("_") && !name.startsWith(".")

  /** List bare data filenames directly inside dir. Empty if dir missing. */
  private def listDataFileNames(fs: FileSystem, dir: Path): Seq[String] =
    try {
      fs.listStatus(dir)
        .filter(s => !s.isDirectory && isDataFile(s.getPath.getName))
        .map(_.getPath.getName)
        .toSeq
    } catch { case _: Exception => Seq.empty }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _started_ FILE WRITERS / PARSERS
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Writes _started_<tid> with job metadata and the list of pending files.
   *
   * Written twice per partition:
   *   1. At newTaskTempFile() — pendingFiles = Seq.empty (files not yet known)
   *   2. At commitTask()      — pendingFiles = actual files written to final location
   *
   * The second write is the critical one: it gives recovery precise knowledge
   * of which files are uncommitted and must be cleaned on the next run.
   */
  private def writeStartedFile(
                                fs:            FileSystem,
                                path:          Path,
                                partitionPath: String,
                                pendingFiles:  Seq[String] = Seq.empty): Unit = {

    val filesJson = pendingFiles.map(f => s""""$f"""").mkString(",")
    val content =
      s"""{
         |  "jobId": "$jobId",
         |  "startedAt": "${Instant.now()}",
         |  "outputPath": "$outputPath",
         |  "partitionPath": "$partitionPath",
         |  "dynamic": $dynamicPartitionOverwrite,
         |  "pendingFiles": [$filesJson]
         |}""".stripMargin

    val out = fs.create(path, /* overwrite= */ true)
    try { out.write(content.getBytes("UTF-8")) }
    finally { out.close() }
  }

  /** Parses _started_ JSON. Returns empty manifest if parsing fails. */
  private def parseStartedFile(path: Path, fs: FileSystem): StartedManifest = {
    try {
      val in = fs.open(path)
      val content = try {
        val buf = new Array[Byte](math.min(in.available().max(0), 4 * 1024 * 1024))
        in.readFully(buf)
        new String(buf, "UTF-8").trim
      } finally { in.close() }

      StartedManifest(
        jobId         = extractJsonString(content, "jobId").getOrElse(jobId),
        partitionPath = extractJsonString(content, "partitionPath").getOrElse(path.getParent.toString),
        startedAt     = extractJsonString(content, "startedAt").getOrElse(""),
        outputPath    = extractJsonString(content, "outputPath").getOrElse(outputPath),
        dynamic       = content.contains("\"dynamic\": true"),
        pendingFiles  = extractJsonStringArray(content, "pendingFiles"))

    } catch {
      case e: Exception =>
        logWarning(s"ManifestCommitProtocol: cannot parse _started_ at $path: ${e.getMessage}")
        StartedManifest(jobId, path.getParent.toString, "", outputPath, false, Seq.empty)
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _committed_ FILE WRITERS / PARSERS
  // ═══════════════════════════════════════════════════════════════════════════

  private def writeCommittedFile(
                                  fs:      FileSystem,
                                  path:    Path,
                                  added:   Seq[String],
                                  removed: Seq[String]): Unit = {

    val addedJson   = added.map(f => s""""$f"""").mkString(",")
    val removedJson = removed.map(f => s""""$f"""").mkString(",")
    writeJson(fs, path, s"""{"added":[$addedJson],"removed":[$removedJson]}""")
  }

  private def writeRootCommittedFile(
                                      rootFs:         FileSystem,
                                      rootDir:        Path,
                                      partitionFiles: java.util.HashMap[String, java.util.List[String]],
                                      allRemoved:     Seq[String]): Unit = {

    val addedRelative = partitionFiles.asScala.flatMap { case (partDir, files) =>
      val rel = partDir.stripPrefix(outputPath).stripPrefix("/")
      files.asScala.map(f => if (rel.isEmpty) f else s"$rel/$f")
    }.toSeq

    val partitionsJson = partitionFiles.asScala.map { case (partDir, files) =>
      val rel  = partDir.stripPrefix(outputPath).stripPrefix("/")
      val fStr = files.asScala.map(f => s""""$f"""").mkString(",")
      s""""$rel":[$fStr]"""
    }.mkString(",")

    val addedStr   = addedRelative.map(f => s""""$f"""").mkString(",")
    val removedStr = allRemoved.map(f => s""""$f"""").mkString(",")
    writeJson(
      rootFs,
      new Path(rootDir, s"_committed_$jobId"),
      s"""{"added":[$addedStr],"removed":[$removedStr],"partitions":{$partitionsJson}}""")
  }

  private def parseCommittedFile(path: Path, fs: FileSystem): CommittedManifest = {
    val in = fs.open(path)
    val content = try {
      val buf = new Array[Byte](math.min(in.available().max(0), 64 * 1024 * 1024))
      in.readFully(buf)
      new String(buf, "UTF-8").trim
    } finally { in.close() }

    CommittedManifest(
      tid          = path.getName.stripPrefix("_committed_"),
      addedFiles   = extractJsonStringArray(content, "added"),
      removedFiles = extractJsonStringArray(content, "removed"))
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  OPTIMIZED PARALLEL DELETES
  //
  //  Deleting many files one-by-one (especially on object stores like S3/ADLS)
  //  is slow — each delete is a separate API call with ~50ms latency.
  //  Using a thread pool gives N× throughput for N parallel threads.
  //
  //  parallelism is configurable. Default = 8.
  //  For ADLS/S3: 16–32 recommended.
  //  For HDFS: 4–8 is sufficient (rename is fast).
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Deletes a list of (FileSystem, Path) pairs in parallel using a thread pool.
   * Errors are logged but do not abort the batch — every file gets an attempt.
   * Returns the count of successfully deleted files.
   */
  private def parallelDelete(
                              targets:     Seq[(FileSystem, Path)],
                              recursive:   Boolean,
                              parallelism: Int,
                              label:       String): Int = {

    if (targets.isEmpty) return 0

    val pool: ExecutorService = Executors.newFixedThreadPool(
      math.min(parallelism, targets.size))

    try {
      val futures: Seq[Future[Boolean]] = targets.map { case (fs, path) =>
        pool.submit(new Callable[Boolean] {
          override def call(): Boolean =
            try {
              if (fs.exists(path)) {
                fs.delete(path, recursive)
                logDebug(s"ManifestCommitProtocol[$label]: deleted $path")
                true
              } else false
            } catch {
              case e: Exception =>
                logWarning(
                  s"ManifestCommitProtocol[$label]: could not delete $path: ${e.getMessage}")
                false
            }
        })
      }

      // Collect results — wait for all, count successes
      futures.count(_.get())

    } finally {
      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.MINUTES)
    }
  }

  /**
   * Builds the (FileSystem, Path) pairs for a flat list of filenames
   * all living in the same directory.
   */
  private def deleteTargetsForDir(
                                   fs:        FileSystem,
                                   dir:       Path,
                                   fileNames: Seq[String]): Seq[(FileSystem, Path)] =
    fileNames.map(name => (fs, new Path(dir, name)))

  // ═══════════════════════════════════════════════════════════════════════════
  //  STATIC OVERWRITE — collect old files before write starts
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Recurses from [dir] downward finding every leaf partition directory
   * (a dir that directly contains data files). Records old filenames per dir.
   * Keys are qualified absolute paths — must match newTaskTempFile's partDir key.
   */
  private def collectOldFilesRecursively(fs: FileSystem, dir: Path): Unit = {
    val statuses =
      try { fs.listStatus(dir) }
      catch { case _: Exception => return }

    val (subDirs, files) = statuses.partition(_.isDirectory)

    val dataFilesHere = files
      .map(_.getPath.getName)
      .filter(isDataFile)

    if (dataFilesHere.nonEmpty) {
      val qualifiedKey = fs.makeQualified(dir).toString
      val list = new java.util.ArrayList[String]()
      dataFilesHere.foreach(list.add)
      pendingDeleteByDir.put(qualifiedKey, list)
      logDebug(
        s"ManifestCommitProtocol.collectOld: $qualifiedKey → ${dataFilesHere.length} files")
    }

    subDirs
      .filterNot(s => { val n = s.getPath.getName; n.startsWith("_") || n.startsWith(".") })
      .foreach(s => collectOldFilesRecursively(fs, s.getPath))
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  EMPTY ANCESTOR CLEANUP (multi-level partitions)
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * After deleting a stale leaf partition dir, walks UP the tree deleting
   * every ancestor that is now empty (has no real content), stopping at stopAt.
   * Handles multi-level partitions: year=2023/month=12/day=05/ → month=12/ → year=2023/
   */
  private def cleanupEmptyAncestors(fs: FileSystem, dir: Path, stopAt: Path): Unit = {
    val qualDir    = try { fs.makeQualified(dir) }    catch { case _: Exception => return }
    val qualStopAt = try { fs.makeQualified(stopAt) } catch { case _: Exception => return }

    if (qualDir == qualStopAt) return
    if (!qualDir.toString.startsWith(qualStopAt.toString)) return

    try {
      if (!fs.exists(qualDir)) return

      val hasRealContent = fs.listStatus(qualDir).exists { s =>
        val n = s.getPath.getName
        !n.startsWith("_") && !n.startsWith(".")
      }

      if (!hasRealContent) {
        fs.delete(qualDir, true)
        logInfo(s"ManifestCommitProtocol: deleted empty ancestor $qualDir")
        cleanupEmptyAncestors(fs, qualDir.getParent, qualStopAt)
      }
    } catch {
      case e: Exception =>
        logWarning(
          s"ManifestCommitProtocol: cleanupEmptyAncestors failed at $qualDir: ${e.getMessage}")
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  RECOVERY — runs at every setupJob
  //
  //  PATH A: _started_ without _committed_ → uncommitted garbage from failed write
  //           Read _started_.pendingFiles → delete each file still on disk
  //           Fallback: scan for files with tid pattern in name
  //
  //  PATH B: _started_ + _committed_ same tid → deferred delete was interrupted
  //           Read _committed_.removed[] → delete each file still on disk
  //           If partition empty after → delete dir + empty ancestors
  //
  //  Both paths are fully idempotent (fs.exists check before every delete).
  // ═══════════════════════════════════════════════════════════════════════════

  private def recoverPendingDeletions(
                                       dir:     Path,
                                       fs:      FileSystem,
                                       rootDir: Path): Unit = {

    val statuses =
      try { fs.listStatus(dir) }
      catch { case _: Exception => return }

    val (subDirs, files) = statuses.partition(_.isDirectory)

    val committedByTid: Map[String, Path] = files
      .filter(_.getPath.getName.startsWith("_committed_"))
      .map(s => s.getPath.getName.stripPrefix("_committed_") -> s.getPath)
      .toMap

    val startedByTid: Map[String, Path] = files
      .filter(_.getPath.getName.startsWith("_started_"))
      .map(s => s.getPath.getName.stripPrefix("_started_") -> s.getPath)
      .toMap

    val parallelism = math.max(deleteParallelism, 4)

    startedByTid.foreach { case (tid, startedPath) =>

      committedByTid.get(tid) match {

        // ── PATH A: uncommitted write (no _committed_) ─────────────────────
        case None =>
          logInfo(
            s"ManifestCommitProtocol.recover[A]: uncommitted write at $dir tid=$tid")
          try {
            val started = parseStartedFile(startedPath, fs)

            // Primary: use pendingFiles list from _started_
            val fromList: Seq[Path] = started.pendingFiles
              .map(name => new Path(dir, name))

            // Fallback: scan for any file whose name contains tid pattern
            // (covers the case where _started_ was written empty before crash)
            val tidPattern = s"-tid-$tid-"
            val fromScan: Seq[Path] = files
              .filter(s => isDataFile(s.getPath.getName) &&
                s.getPath.getName.contains(tidPattern) &&
                !started.pendingFiles.contains(s.getPath.getName))
              .map(_.getPath)
              .toSeq

            val targets = (fromList ++ fromScan)
              .distinct
              .map(p => (fs, p))

            val deleted = parallelDelete(targets, recursive = false, parallelism, s"recoverA[$dir]")
            safeDelete(fs, startedPath)
            logInfo(
              s"ManifestCommitProtocol.recover[A]: complete at $dir tid=$tid " +
                s"deleted=$deleted/${targets.size}")

          } catch {
            case e: Exception =>
              logWarning(
                s"ManifestCommitProtocol.recover[A]: failed at $dir tid=$tid: ${e.getMessage}")
          }

        // ── PATH B: deferred delete interrupted (_started_ + _committed_) ──
        case Some(committedPath) =>
          logInfo(
            s"ManifestCommitProtocol.recover[B]: deferred delete at $dir tid=$tid")
          try {
            val manifest = parseCommittedFile(committedPath, fs)

            val targets = manifest.removedFiles
              .filter(isDataFile)               // skip _SUCCESS etc.
              .map(name => (fs, new Path(dir, name)))

            val deleted = parallelDelete(targets, recursive = false, parallelism, s"recoverB[$dir]")

            // If no data remains and no newer _committed_ from another job exists,
            // the partition is stale — delete dir and empty ancestors
            val remainingData =
            try { fs.listStatus(dir).exists(s => isDataFile(s.getPath.getName)) }
            catch { case _: Exception => true }

            if (!remainingData) {
              val hasNewerCommit = committedByTid.keys.exists(_ != tid)
              if (!hasNewerCommit) {
                try {
                  if (fs.exists(dir)) {
                    fs.delete(dir, true)
                    logInfo(s"ManifestCommitProtocol.recover[B]: deleted empty stale $dir")
                    cleanupEmptyAncestors(fs, dir.getParent, rootDir)
                  }
                } catch {
                  case e: Exception =>
                    logWarning(
                      s"ManifestCommitProtocol.recover[B]: cannot delete stale $dir: " +
                        s"${e.getMessage}")
                }
                return  // dir gone — skip _started_ delete below
              }
            }

            safeDelete(fs, startedPath)
            logInfo(
              s"ManifestCommitProtocol.recover[B]: complete at $dir tid=$tid " +
                s"deleted=$deleted/${targets.size}")

          } catch {
            case e: Exception =>
              logWarning(
                s"ManifestCommitProtocol.recover[B]: failed at $dir tid=$tid: ${e.getMessage}")
          }
      }
    }

    // Recurse into partition subdirs
    subDirs
      .filterNot(s => { val n = s.getPath.getName; n.startsWith("_") || n.startsWith(".") })
      .foreach(s => recoverPendingDeletions(s.getPath, fs, rootDir))
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)

    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)

    // Read delete parallelism from Spark conf
    val confParallelism = jobContext.getConfiguration
      .getInt("spark.sql.manifest.deleteParallelism", 8)
    // Note: deleteParallelism is a val — we store separately for use in commitJob
    // (it is already set to 8 as default; override at construction if needed)

    // ── Recovery: complete any pending work from previous failed run ────────
    recoverPendingDeletions(outputDir, jobFs, outputDir)

    // ── Start new write ────────────────────────────────────────────────────
    cleanupOrphanedStarted(outputDir, jobFs)
    writeStartedFile(jobFs, new Path(outputDir, s"_started_$jobId"),
      partitionPath = outputPath)

    logInfo(
      s"ManifestCommitProtocol.setupJob: path=$outputPath jobId=$jobId " +
        s"dynamic=$dynamicPartitionOverwrite")
  }

  /**
   * Called by InsertIntoHadoopFsRelationCommand for STATIC overwrite only.
   * Defers deletion: captures old file list now, executes deletion in commitJob.
   */
  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    collectOldFilesRecursively(fs, path)
    logInfo(
      s"ManifestCommitProtocol.deleteWithJob: deferred $path → " +
        s"${pendingDeleteByDir.size()} partition dir(s) captured")
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    val parallelism = deleteParallelism

    // ── Step 1: unwrap CombinedCommitPayload ────────────────────────────────
    val mergedPartitionFiles =
      new java.util.HashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayloadV4 =>
          cp.partitionFiles.forEach { (partDir, files) =>
            mergedPartitionFiles
              .computeIfAbsent(partDir, _ => new java.util.ArrayList[String]())
              .addAll(files)
          }
          new TaskCommitMessage(cp.superObj)
        case _ => msg
      }
    }

    // ── Step 2: capture REMOVED lists BEFORE super.commitJob() ─────────────
    //
    // DYNAMIC: list old files NOW before super.commitJob() deletes them.
    // STATIC:  already captured in pendingDeleteByDir by collectOldFilesRecursively.
    val removedByPartition =
    new java.util.HashMap[String, java.util.List[String]]()

    val newFileSet = mergedPartitionFiles.asScala.flatMap(f => f._2.asScala).toSet
    if (dynamicPartitionOverwrite) {
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p      = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        val old    = listDataFileNames(partFs, partFs.makeQualified(p)).filterNot(newFileSet.contains)
        if (old.nonEmpty) {
          removedByPartition.put(partDir, old.asJava)
        }
      }
    } else {
      // Static: split into written (removed by file name) and stale (removed by dir)
      pendingDeleteByDir.forEach { (qualDir, oldFiles) =>
        if (!oldFiles.isEmpty) {
          removedByPartition.put(qualDir, oldFiles)
        }
      }
    }

    // ── Step 3: super.commitJob() ────────────────────────────────────────────
    // DYNAMIC: deletes old partition dirs, renames staging → final.
    // STATIC:  renames staged files to final (old files still present).
    // _committed_ MUST be written AFTER this.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: write per-partition _committed_ (AFTER super) ───────────────
    //
    // Written after super so that:
    //   DYNAMIC: partition dirs now exist at final location (super created them)
    //   STATIC:  new files now at final location alongside old ones
    mergedPartitionFiles.forEach { (partDir, addedFiles) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      val removed  = removedByPartition.getOrDefault(
        partDir, new java.util.ArrayList[String]())
      writeCommittedFile(
        partFs,
        new Path(partPath, s"_committed_$jobId"),
        addedFiles.asScala.toSeq,
        removed.asScala.toSeq)
    }

    // ── Step 5: write root-level _committed_ ────────────────────────────────
    val allRemoved = removedByPartition.values().asScala.flatMap(_.asScala).toSeq
    writeRootCommittedFile(jobFs, outputDir, mergedPartitionFiles, allRemoved)

    // ── Step 6: OPTIMIZED PARALLEL deletes of old data ──────────────────────
    if (!dynamicPartitionOverwrite) {
      // Build two work lists:
      //   A) partitions written: delete old files by name (keep dir + _committed_)
      //   B) stale partitions:   delete entire dir + clean empty ancestors
      val fileDeleteTargets = mutable.ArrayBuffer[(FileSystem, Path)]()
      val staleDirs         = mutable.ArrayBuffer[(FileSystem, Path)]()

      pendingDeleteByDir.forEach { (qualDir, oldFileNames) =>
        val dirPath = new Path(qualDir)
        val dirFs   = dirPath.getFileSystem(jobContext.getConfiguration)

        if (mergedPartitionFiles.containsKey(qualDir)) {
          // Written partition: delete only the specific old files
          oldFileNames.forEach { name =>
            fileDeleteTargets += ((dirFs, new Path(dirPath, name)))
          }
        } else {
          // Stale partition: delete entire dir
          staleDirs += ((dirFs, dirPath))
        }
      }

      // Parallel delete of old data files in written partitions
      if (fileDeleteTargets.nonEmpty) {
        val deleted = parallelDelete(
          fileDeleteTargets.toSeq, recursive = false, parallelism, "step6-files")
        logInfo(
          s"ManifestCommitProtocol.commitJob: deleted $deleted/${fileDeleteTargets.size} " +
            s"old data files (parallel, threads=$parallelism)")
      }

      // Parallel delete of stale partition dirs
      if (staleDirs.nonEmpty) {
        val deleted = parallelDelete(
          staleDirs.toSeq, recursive = true, parallelism, "step6-staleDirs")
        logInfo(
          s"ManifestCommitProtocol.commitJob: deleted $deleted/${staleDirs.size} " +
            s"stale partition dirs")

        // Clean up empty ancestor dirs for every stale partition removed
        // (serial — cheap because ancestor count is small)
        val rootPath = new Path(outputPath)
        staleDirs.foreach { case (fs, dirPath) =>
          cleanupEmptyAncestors(fs, dirPath.getParent, rootPath)
        }
      }

      pendingDeleteByDir.clear()
    }

    // ── Step 7: delete _started_ files (write is fully complete) ────────────
    //
    // Root _started_: always delete
    safeDelete(jobFs, new Path(outputDir, s"_started_$jobId"))

    if (dynamicPartitionOverwrite) {
      // Dynamic: super deleted old partition dirs entirely in step 3.
      // Partition-level _started_ is gone with them. Clean residual old manifests.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p   = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifests(p, pFs)
      }
    } else {
      // Static: partition dirs for written partitions still exist.
      // Stale partition dirs were deleted entirely in step 6.
      // Delete _started_ only for the written partitions.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p   = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        safeDelete(pFs, new Path(p, s"_started_$jobId"))
      }
    }

    logInfo(
      s"ManifestCommitProtocol.commitJob: COMPLETE " +
        s"path=$outputPath dynamic=$dynamicPartitionOverwrite " +
        s"partitions_written=${mergedPartitionFiles.size()} " +
        s"total_added=${mergedPartitionFiles.values().asScala.map(_.size()).sum} " +
        s"total_removed=${allRemoved.size}")
  }

  override def abortJob(jobContext: JobContext): Unit = {
    // Discard deferred deletes — old data MUST be preserved on failure
    pendingDeleteByDir.clear()
    super.abortJob(jobContext)
    // Leave _started_ in place — signals failed write to ManifestAwareFileIndex
    logInfo(s"ManifestCommitProtocol.abortJob: old data preserved at $outputPath")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  //  Object arrives here after Java deserialization: @transient fields = null.
  //  setupTask() is the ONLY safe place to initialize executor-side state.
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  /**
   * Spark 3.5 calls the FileNameSpec variant (NOT the deprecated ext: String one).
   * Override this or partition-level _started_ is never written and
   * taskPartitionFiles stays empty.
   */
  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir: Option[String],
                                spec: FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFile(taskContext, dir, spec)

    // Compute QUALIFIED final partition dir — must match pendingDeleteByDir keys
    val rawPartDir = dir match {
      case Some(d) => new Path(outputPath, d)
      case None    => new Path(outputPath)
    }
    val partFs  = rawPartDir.getFileSystem(taskContext.getConfiguration)
    val partDir = partFs.makeQualified(rawPartDir).toString

    // Write partition-level _started_ on first file in this partition (this task)
    // pendingFiles is empty here — will be filled in at commitTask
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      cleanupOrphanedStarted(partPath, partFs)
      writeStartedFile(partFs, new Path(partPath, s"_started_$jobId"),
        partitionPath = partDir,
        pendingFiles  = Seq.empty)
      logDebug(
        s"ManifestCommitProtocol: wrote _started_(empty) at $partDir " +
          s"task=${taskContext.getTaskAttemptID}")
    }

    // Track filename under final partition dir (not staging)
    taskPartitionFiles
      .computeIfAbsent(partDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)

    stagingPath
  }

  override def newTaskTempFileAbsPath(
                                       taskContext: TaskAttemptContext,
                                       absoluteDir: String,
                                       spec: FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    val rawPath     = new Path(absoluteDir)
    val qualDir     = rawPath.getFileSystem(taskContext.getConfiguration)
      .makeQualified(rawPath).toString
    taskPartitionFiles
      .computeIfAbsent(qualDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)
    stagingPath
  }

  /**
   * Called on executor after all files for this task are written to final location.
   *
   * KEY STEP: overwrites _started_ with the ACTUAL list of files now at
   * final location. This gives recovery precise knowledge of which files
   * are uncommitted and must be deleted if commitJob never runs.
   */
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    // super.commitTask() moves files staging → final and returns Tuple2
    val superMsg = super.commitTask(taskContext)

    // Snapshot partition → files
    val snapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (partDir, files) =>
      snapshot.put(partDir, new java.util.ArrayList[String](files))
    }

    // ── Overwrite _started_ with actual pendingFiles ────────────────────────
    // Files are now at their final location. _started_ now contains the exact
    // list that recovery must delete if the cluster dies before commitJob.
    snapshot.forEach { (partDir, files) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(taskContext.getConfiguration)
      writeStartedFile(
        partFs,
        new Path(partPath, s"_started_$jobId"),
        partitionPath = partDir,
        pendingFiles  = files.asScala.toSeq)
      logDebug(
        s"ManifestCommitProtocol: updated _started_ with ${files.size()} " +
          s"pendingFiles at $partDir task=${taskContext.getTaskAttemptID}")
    }

    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    new TaskCommitMessage(CombinedCommitPayloadV4(superMsg.obj, snapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
    // Leave _started_ — recovery PATH A will clean up pendingFiles on next run
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  UTILITIES
  // ═══════════════════════════════════════════════════════════════════════════

  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning(
        s"ManifestCommitProtocol: executor state null — invoking setupTask() " +
          s"task=${taskContext.getTaskAttemptID}")
      setupTask(taskContext)
    }
  }

  private def cleanupOrphanedStarted(dir: Path, fs: FileSystem): Unit =
    try {
      fs.listStatus(dir)
        .filter(_.getPath.getName.startsWith("_started_"))
        .foreach { s =>
          val tid = s.getPath.getName.stripPrefix("_started_")
          if (!fs.exists(new Path(dir, s"_committed_$tid"))) {
            fs.delete(s.getPath, false)
            logInfo(s"ManifestCommitProtocol: removed orphaned ${s.getPath}")
          }
        }
    } catch { case _: Exception => () }

  private def cleanupOldManifests(dir: Path, fs: FileSystem): Unit =
    try {
      fs.listStatus(dir)
        .filter { s =>
          val n = s.getPath.getName
          (n.startsWith("_committed_") || n.startsWith("_started_")) &&
            !n.endsWith(jobId)
        }
        .foreach(s => safeDelete(fs, s.getPath))
    } catch { case _: Exception => () }

  private def safeDelete(fs: FileSystem, path: Path): Unit =
    try { fs.delete(path, false) }
    catch { case e: Exception =>
      logWarning(s"ManifestCommitProtocol: could not delete $path: ${e.getMessage}") }

  private def writeJson(fs: FileSystem, path: Path, json: String): Unit = {
    val out = fs.create(path, true)
    try { out.write(json.getBytes("UTF-8")) } finally { out.close() }
  }

  private def extractJsonString(json: String, key: String): Option[String] = {
    val p = s""""$key"\\s*:\\s*"([^"]*)"""".r
    p.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractJsonStringArray(json: String, key: String): Seq[String] = {
    val p = s""""$key"\\s*:\\s*\\[([^\\]]*)\\]""".r
    p.findFirstMatchIn(json) match {
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
//```

//---

//## Complete lifecycle — what every file contains at every moment
//```
//WRITE in progress (tasks running):
//
//  table/_started_7628                     ← job-level, written at setupJob
//{
//  "jobId": "7628",
//  "startedAt": "2024-04-11T09:32:15Z",
//  "outputPath": "s3://bucket/table",
//  "partitionPath": "s3://bucket/table",
//  "dynamic": false,
//  "pendingFiles": []                    ← empty, no data at job level
//}
//
//table/p=1/_started_7628                 ← written at first newTaskTempFile in p=1
//{"pendingFiles": []}                    ← still empty
//
//↓ commitTask fires:
//
//  table/p=1/_started_7628                 ← OVERWRITTEN with actual files
//  {
//    "pendingFiles": [
//    "part-00000-tid-7628-...-c000.snappy.parquet",
//    "part-00001-tid-7628-...-c000.snappy.parquet"
//    ]
//  }
//
//COMMIT complete (commitJob finished):
//
//  table/_committed_7628
//    {"added":["p=1/part-00000...", "p=2/part-00001..."],
//      "removed":["p=1/old1.parquet", "p=3/old3.parquet"],
//      "partitions":{"p=1":[...],"p=2":[...]}}
//
//table/p=1/_committed_7628
//{"added":["part-00000-tid-7628.parquet"],
//  "removed":["old1.parquet"]}
//
//table/p=1/_started_7628                 ← DELETED in step 7
//table/_started_7628                     ← DELETED in step 7
//
//
//CRASH during write (between commitTask and commitJob):
//
//  table/p=1/_started_7628
//{"pendingFiles":["part-00000-tid-7628.parquet"]}   ← exact file to delete
//  table/p=1/part-00000-tid-7628.parquet              ← uncommitted garbage
//
//Next setupJob → recoverPendingDeletions → PATH A:
//  Read _started_.pendingFiles = ["part-00000-tid-7628.parquet"]
//delete part-00000-tid-7628.parquet ✓
//  delete _started_7628 ✓
//Table clean ✓
//
//CRASH during deletion (step 6, between _committed_ written and old files deleted):
//
//  table/p=1/_committed_7628
//  {"added":["part-NEW.parquet"],"removed":["part-OLD.parquet"]}
//
//  table/p=1/_started_7628                  ← still exists (step 7 never ran)
//  table/p=1/part-NEW.parquet               ← new data (committed)
//  table/p=1/part-OLD.parquet               ← old data (deletion was interrupted)
//
//  Next setupJob → recoverPendingDeletions → PATH B:
//    Read _committed_.removed = ["part-OLD.parquet"]
//  delete part-OLD.parquet ✓
//    partition still has data (part-NEW.parquet) → keep dir
//    delete _started_7628 ✓
//  Table clean ✓
//  ```
//
//  ---
//
//  ## Optimized delete performance
//    ```
//  Configuration:
//    spark.sql.manifest.deleteParallelism = 32  (ADLS/S3)
//  spark.sql.manifest.deleteParallelism = 8   (HDFS default)
//
//  Throughput comparison (1000 files to delete, 50ms latency per call):
//    Serial:    1000 × 50ms = 50 seconds
//    8 threads:  1000/8 × 50ms = 6.25 seconds  (8×)
//  32 threads: 1000/32 × 50ms = 1.56 seconds (32×)
//
//  parallelDelete is used in:
//    step 6 Case A: old data files in written partitions
//    step 6 Case B: stale partition dirs (recursive delete)
//  recoverPendingDeletions PATH A: uncommitted garbage files
//  recoverPendingDeletions PATH B: deferred old-data files


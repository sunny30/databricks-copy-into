package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest



import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.{TaskCommitMessage}
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import java.time.Instant
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.JavaConverters._
import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
//  Top-level types
//  Defined OUTSIDE the class so Java serialization/deserialization preserves
//  class identity and pattern matching works on the driver after round-trip.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Payload inside every TaskCommitMessage.
 * Carries super's original Tuple2 alongside our partition→files map so that
 * super.commitJob() can cast its messages correctly while we extract our data.
 */
final case class CombinedCommitPayloadV6(
                                        superObj:       Any,
                                        partitionFiles: java.util.HashMap[String, java.util.List[String]]
                                      )

/**
 * Parsed content of _started_<jobId>.
 *
 * DBIO alignment:
 *   _started_ is the job-level transaction lock signal.
 *   It is written at setupJob (root) and at newTaskTempFile (partition, first file).
 *   It is UPDATED at commitTask with pendingFiles — the actual data files written
 *   by this task that are now at final location.
 *   pendingFiles makes _started_ a recovery source independent of _pending_ files.
 */
final case class StartedManifestV6(
                                  jobId:         String,
                                  partitionPath: String,
                                  startedAt:     String,
                                  outputPath:    String,
                                  dynamic:       Boolean,
                                  pendingFiles:  Seq[String]   // files at final location, not yet job-committed
                                )

/**
 * Parsed content of _pending_<jobId>_<taskAttemptId>.
 *
 * One file per task per partition.  Never overwritten by another task.
 * Recovery PATH A unions all _pending_<jobId>_* in a dir for complete coverage.
 */
final case class PendingTaskManifestV6(
                                      jobId:         String,
                                      taskAttemptId: String,
                                      partitionPath: String,
                                      startedAt:     String,
                                      pendingFiles:  Seq[String]
                                    )

/**
 * Parsed content of _committed_<jobId>.
 *
 * DBIO alignment:
 *   added[]   — files that are valid and readable for this transaction
 *   removed[] — files that were present before this transaction and are now deleted
 *   Both lists are the durable recovery record for PATH B.
 */
final case class CommittedManifestV6(
                                    tid:          String,
                                    addedFiles:   Seq[String],
                                    removedFiles: Seq[String]
                                  )

// ─────────────────────────────────────────────────────────────────────────────
//  ManifestCommitProtocol
//
//  DBIO-compatible, petabyte-scale open-source Spark 3.5 commit protocol.
//
//  File layout per partition dir (matches Databricks DBIO):
//    _started_<jobId>                    job-level lock · minimal + pendingFiles
//    _pending_<jobId>_<taskAttemptId>    per-task uncommitted file list
//    _committed_<jobId>                  committed manifest: added[] + removed[]
//
//  Three-source recovery on next setupJob():
//    PATH A (_started_ only):          delete all files across _pending_ + _started_ + tid-scan
//    PATH B (_started_ + _committed_): complete deferred old-file deletion
//
//  Petabyte-scale properties:
//    All dir traversal is iterative BFS (no stack overflow)
//    Single shared thread pool per job (no repeated create/shutdown)
//    Driver stores dir paths only, not filenames (no OOM)
//    File listing is lazy (step 2 of commitJob, not at deleteWithJob time)
//    Manifest writes, deletes, listings all parallel
//    deleteParallelism configurable via spark.sql.manifest.deleteParallelism
// ─────────────────────────────────────────────────────────────────────────────

class ManifestFileCommitProtocolV6(
                              jobId:                    String,
                              outputPath:               String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ═══════════════════════════════════════════════════════════════════════════
  //  SERIALIZATION CONTRACT
  //
  //  The committer object is Java-serialized on the DRIVER and sent to each
  //  EXECUTOR.  @transient fields are NULL after deserialization.
  //
  //  DRIVER-side fields  → initialize in setupJob()
  //  EXECUTOR-side fields → initialize in setupTask()  ← NEVER inline
  // ═══════════════════════════════════════════════════════════════════════════

  // ── DRIVER-SIDE ─────────────────────────────────────────────────────────────
  @transient private var outputDir:         Path            = _
  @transient private var jobFs:             FileSystem      = _
  @transient private var sharedPool:        ExecutorService = _
  @transient private var deleteParallelism: Int             = 16
  @transient private var writeParallelism:  Int             = 16

  // Stores ONLY directory paths (qualified absolute), never filenames.
  // Avoids driver OOM on petabyte tables with billions of old files.
  // Old filenames are re-listed lazily in commitJob step 2.
  @transient private val pendingDeleteDirs =
  java.util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]())

  // ── EXECUTOR-SIDE ── initialized in setupTask(), never inline ────────────
  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String, java.util.concurrent.CopyOnWriteArrayList[String]] = _

  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ═══════════════════════════════════════════════════════════════════════════
  //  PRIMITIVES
  // ═══════════════════════════════════════════════════════════════════════════

  @inline private def isDataFile(name: String): Boolean =
    name.nonEmpty && !name.startsWith("_") && !name.startsWith(".")

  private def listDataFileNames(fs: FileSystem, dir: Path): Seq[String] =
    try {
      fs.listStatus(dir)
        .withFilter(s => !s.isDirectory && isDataFile(s.getPath.getName))
        .map(_.getPath.getName)
    } catch { case _: Exception => Array.empty[String] }

  private def readFileContent(path: Path, fs: FileSystem, maxBytes: Int): String = {
    val in = fs.open(path)
    try {
      val sz  = math.min(math.max(in.available(), 0), maxBytes)
      val buf = new Array[Byte](sz)
      in.readFully(buf)
      new String(buf, "UTF-8").trim
    } finally { in.close() }
  }

  private def writeJson(fs: FileSystem, path: Path, json: String): Unit = {
    val out = fs.create(path, /* overwrite= */ true)
    try { out.write(json.getBytes("UTF-8")) } finally { out.close() }
  }

  private def safeDelete(fs: FileSystem, path: Path): Unit =
    try { fs.delete(path, false) }
    catch { case e: Exception =>
      logWarning(s"ManifestCommitProtocol: cannot delete $path: ${e.getMessage}") }

  private def extractJsonString(json: String, key: String): Option[String] =
    s""""$key"\\s*:\\s*"([^"]*)"""".r.findFirstMatchIn(json).map(_.group(1))

  private def extractJsonStringArray(json: String, key: String): Seq[String] = {
    s""""$key"\\s*:\\s*\\[([^\\]]*)\\]""".r.findFirstMatchIn(json) match {
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

  // ═══════════════════════════════════════════════════════════════════════════
  //  PARALLEL EXECUTION
  //  Single shared pool per job — created in setupJob, shutdown in commitJob.
  // ═══════════════════════════════════════════════════════════════════════════

  private def submitTask[T](body: => T): Future[T] =
    sharedPool.submit(new Callable[T] { def call(): T = body })

  /**
   * Runs seq of tasks in parallel on sharedPool.
   * Collects all results; logs but does not re-throw errors so one failure
   * never aborts an entire batch (e.g. one file delete failure does not
   * prevent the remaining 999 from completing).
   */
  private def parallelExec[T](tasks: Seq[() => T], label: String): Seq[T] = {
    if (tasks.isEmpty) return Seq.empty
    val futures = tasks.map(t => submitTask(t()))
    futures.flatMap { f =>
      try { Some(f.get()) }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol[$label]: ${e.getMessage}")
        None
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _started_<jobId>  WRITE / PARSE
  //
  //  DBIO: _started_ is the in-progress transaction lock.
  //        It is written once at setupJob (root) and once per partition at
  //        newTaskTempFile (first file per partition per task).
  //        It is OVERWRITTEN at commitTask to add pendingFiles so that
  //        recovery PATH A has a reliable file list even if _pending_ was
  //        never written (crash between staging→final rename and _pending_ write).
  // ═══════════════════════════════════════════════════════════════════════════

  private def writeStartedFile(
                                fs:            FileSystem,
                                path:          Path,
                                partitionPath: String,
                                pendingFiles:  Seq[String] = Seq.empty): Unit = {

    val filesJson = pendingFiles.map(f => s""""$f"""").mkString(",")
    writeJson(fs, path,
      s"""{
         |  "jobId": "$jobId",
         |  "startedAt": "${Instant.now()}",
         |  "outputPath": "$outputPath",
         |  "partitionPath": "$partitionPath",
         |  "dynamic": $dynamicPartitionOverwrite,
         |  "pendingFiles": [$filesJson]
         |}""".stripMargin)
  }

  private def parseStartedFile(path: Path, fs: FileSystem): StartedManifestV6 =
    try {
      val c = readFileContent(path, fs, 4 * 1024 * 1024)
      StartedManifestV6(
        jobId         = extractJsonString(c, "jobId").getOrElse(jobId),
        partitionPath = extractJsonString(c, "partitionPath").getOrElse(path.getParent.toString),
        startedAt     = extractJsonString(c, "startedAt").getOrElse(""),
        outputPath    = extractJsonString(c, "outputPath").getOrElse(outputPath),
        dynamic       = c.contains("\"dynamic\": true"),
        pendingFiles  = extractJsonStringArray(c, "pendingFiles"))
    } catch {
      case e: Exception =>
        logWarning(s"ManifestCommitProtocol: cannot parse _started_ $path: ${e.getMessage}")
        StartedManifestV6(jobId, path.getParent.toString, "", outputPath, false, Seq.empty)
    }

  /**
   * Overwrites partition _started_<jobId> to add this task's files.
   * Reads existing content first to union files written by prior tasks
   * in the same partition for the same job.
   *
   * Called BEFORE super.commitTask() (before staging→final rename) so that
   * if the cluster crashes after rename but before _pending_ is written,
   * recovery PATH A finds these filenames in _started_ and deletes them.
   */
  private def updatePartitionStarted(
                                      partDir:     String,
                                      files:       Seq[String],
                                      taskContext: TaskAttemptContext): Unit = {

    val partPath    = new Path(partDir)
    val partFs      = partPath.getFileSystem(taskContext.getConfiguration)
    val startedPath = new Path(partPath, s"_started_$jobId")

    val existing: Seq[String] = try {
      if (partFs.exists(startedPath))
        parseStartedFile(startedPath, partFs).pendingFiles
      else Seq.empty
    } catch { case _: Exception => Seq.empty }

    writeStartedFile(partFs, startedPath, partDir, (existing ++ files).distinct)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _pending_<jobId>_<taskAttemptId>  WRITE / PARSE
  //
  //  One file per task per partition.  Never overwritten by another task.
  //  Written AFTER super.commitTask() so filenames are confirmed at final location.
  //  Recovery PATH A unions all _pending_<jobId>_* in a dir.
  // ═══════════════════════════════════════════════════════════════════════════

  @inline private def pendingFileName(taskAttemptId: String) =
    s"_pending_${jobId}_$taskAttemptId"

  private def writePendingFile(
                                fs:            FileSystem,
                                partDir:       Path,
                                taskAttemptId: String,
                                files:         Seq[String]): Unit = {

    val filesJson = files.map(f => s""""$f"""").mkString(",")
    writeJson(fs, new Path(partDir, pendingFileName(taskAttemptId)),
      s"""{
         |  "jobId": "$jobId",
         |  "taskAttemptId": "$taskAttemptId",
         |  "partitionPath": "${partDir.toString}",
         |  "startedAt": "${Instant.now()}",
         |  "pendingFiles": [$filesJson]
         |}""".stripMargin)
  }

  private def parsePendingFile(path: Path, fs: FileSystem): PendingTaskManifestV6 =
    try {
      val c = readFileContent(path, fs, 64 * 1024 * 1024)
      PendingTaskManifestV6(
        jobId         = extractJsonString(c, "jobId").getOrElse(jobId),
        taskAttemptId = extractJsonString(c, "taskAttemptId").getOrElse(""),
        partitionPath = extractJsonString(c, "partitionPath").getOrElse(path.getParent.toString),
        startedAt     = extractJsonString(c, "startedAt").getOrElse(""),
        pendingFiles  = extractJsonStringArray(c, "pendingFiles"))
    } catch {
      case e: Exception =>
        logWarning(s"ManifestCommitProtocol: cannot parse _pending_ $path: ${e.getMessage}")
        PendingTaskManifestV6(jobId, "", path.getParent.toString, "", Seq.empty)
    }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _committed_<jobId>  WRITE / PARSE
  //
  //  DBIO: _committed_ is the durable success record.
  //  added[]   — files readable by this transaction
  //  removed[] — files that existed before and are being superseded
  //  Recovery PATH B uses removed[] to complete interrupted deletions.
  // ═══════════════════════════════════════════════════════════════════════════

  private def writePartitionCommittedFile(
                                           fs:      FileSystem,
                                           partDir: Path,
                                           added:   Seq[String],
                                           removed: Seq[String]): Unit = {

    val a = added.map(f => s""""$f"""").mkString(",")
    val r = removed.map(f => s""""$f"""").mkString(",")
    writeJson(fs, new Path(partDir, s"_committed_$jobId"),
      s"""{"added":[$a],"removed":[$r]}""")
  }

  private def writeRootCommittedFile(
                                      rootFs:         FileSystem,
                                      rootDir:        Path,
                                      partitionFiles: java.util.HashMap[String, java.util.List[String]],
                                      allRemoved:     Seq[String]): Unit = {

    val addedRelative = partitionFiles.asScala.flatMap { case (pDir, files) =>
      val rel = pDir.stripPrefix(outputPath).stripPrefix("/")
      files.asScala.map(f => if (rel.isEmpty) f else s"$rel/$f")
    }.toSeq

    val partitionsJson = partitionFiles.asScala.map { case (pDir, files) =>
      val rel  = pDir.stripPrefix(outputPath).stripPrefix("/")
      val fStr = files.asScala.map(f => s""""$f"""").mkString(",")
      s""""$rel":[$fStr]"""
    }.mkString(",")

    val a = addedRelative.map(f => s""""$f"""").mkString(",")
    val r = allRemoved.map(f => s""""$f"""").mkString(",")

    writeJson(rootFs, new Path(rootDir, s"_committed_$jobId"),
      s"""{"added":[$a],"removed":[$r],"partitions":{$partitionsJson}}""")
  }

  private def parseCommittedFile(path: Path, fs: FileSystem): CommittedManifestV6 = {
    val c = readFileContent(path, fs, 256 * 1024 * 1024)
    CommittedManifestV6(
      tid          = path.getName.stripPrefix("_committed_"),
      addedFiles   = extractJsonStringArray(c, "added"),
      removedFiles = extractJsonStringArray(c, "removed"))
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  STATIC OVERWRITE — collect old dirs (NOT files) before write
  //
  //  Iterative BFS.  Stores only dir paths to avoid driver OOM.
  //  Old filenames are re-listed lazily in commitJob step 2.
  // ═══════════════════════════════════════════════════════════════════════════

  private def collectOldDirsBFS(fs: FileSystem, startDir: Path): Unit = {
    val queue = new java.util.ArrayDeque[Path]()
    queue.add(startDir)

    while (!queue.isEmpty) {
      val dir      = queue.poll()
      val statuses = try { fs.listStatus(dir) }
      catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      if (files.exists(s => isDataFile(s.getPath.getName))) {
        pendingDeleteDirs.add(fs.makeQualified(dir).toString)
        logDebug(s"ManifestCommitProtocol.collectOld: $dir")
      }

      subDirs
        .withFilter(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  EMPTY ANCESTOR CLEANUP — iterative upward walk
  //
  //  After a stale leaf partition dir is deleted, walks UP the tree and removes
  //  every ancestor that is now empty, stopping at outputPath.
  //  Handles arbitrarily deep multi-level partitions (year/month/day/hour/...).
  // ═══════════════════════════════════════════════════════════════════════════

  private def cleanupEmptyAncestorsBFS(
                                        fs:      FileSystem,
                                        startAt: Path,
                                        stopAt:  Path): Unit = {

    val qualStop = try { fs.makeQualified(stopAt) }
    catch { case _: Exception => return }
    var current  = try { fs.makeQualified(startAt) }
    catch { case _: Exception => return }

    while (current != qualStop &&
      current.toString.startsWith(qualStop.toString)) {
      try {
        if (!fs.exists(current)) {
          current = current.getParent
        } else {
          val hasReal = fs.listStatus(current)
            .exists(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
          if (hasReal) return  // sibling content exists — stop
          fs.delete(current, true)
          logInfo(s"ManifestCommitProtocol: deleted empty ancestor $current")
          current = current.getParent
        }
      } catch {
        case e: Exception =>
          logWarning(
            s"ManifestCommitProtocol: cleanupEmptyAncestors failed at $current: ${e.getMessage}")
          return
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  RECOVERY — iterative BFS, parallel per-dir tasks
  //
  //  Called at setupJob() BEFORE cleanupOrphanedStarted so recovery
  //  can read the _started_ files it needs.
  //
  //  PATH A  _started_<tid> present, no _committed_<tid>:
  //    Three-source file collection (union, distinct):
  //      S1 _pending_<tid>_*  pendingFiles   ← authoritative (post-rename)
  //      S2 _started_<tid>    pendingFiles   ← backup (pre-rename)
  //      S3 tid-pattern scan  filename match ← last resort
  //    Delete all found files in parallel.
  //    Delete all _pending_<tid>_* and _started_<tid>.
  //
  //  PATH B  _started_<tid> + _committed_<tid> both present:
  //    Read _committed_.removed[] → delete each file still on disk.
  //    If partition empty after and no newer _committed_ → delete dir + ancestors.
  //    Delete _started_<tid>.
  //
  //  Both paths are fully idempotent: fs.exists() guard before every delete.
  // ═══════════════════════════════════════════════════════════════════════════

  private def recoverBFS(rootDir: Path, fs: FileSystem): Unit = {
    val rootPath  = new Path(outputPath)
    val bfsQueue  = new LinkedBlockingQueue[Path]()
    val futures   = new java.util.concurrent.ConcurrentLinkedQueue[Future[_]]()

    bfsQueue.add(rootDir)

    // BFS: scan dirs, submit recovery work in parallel, keep BFS going
    while (!bfsQueue.isEmpty) {
      val dir      = bfsQueue.poll()
      val statuses = try { fs.listStatus(dir) }
      catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      // Index manifest files in this dir by tid
      val committedByTid: Map[String, Path] = files
        .withFilter(_.getPath.getName.startsWith("_committed_"))
        .map(s => s.getPath.getName.stripPrefix("_committed_") -> s.getPath)
        .toMap

      val startedByTid: Map[String, Path] = files
        .withFilter(_.getPath.getName.startsWith("_started_"))
        .map(s => s.getPath.getName.stripPrefix("_started_") -> s.getPath)
        .toMap

      // _pending_<tid>_* grouped by tid
      val pendingByTid: Map[String, Seq[Path]] =
        files
          .filter { s =>
            val name = s.getPath.getName
            val parts = name.stripPrefix("_pending_").split("_")
            name.startsWith("_pending_") && parts.length >= 2
          }
          .groupBy { s =>
            s.getPath.getName.stripPrefix("_pending_").split("_").head
          }
          .map { case (tid, seq) =>
            tid -> seq.map(_.getPath).toSeq
          }

      // Snapshot files list for use inside lambda (avoid capturing mutable state)
      val filesList = files.toSeq

      // Submit recovery for each tid that has a _started_ in this dir
      startedByTid.foreach { case (tid, startedPath) =>
        val dirCapture         = dir
        val committedPathOpt   = committedByTid.get(tid)
        val pendingPaths       = pendingByTid.getOrElse(tid, Seq.empty)
        val allCommittedCapt   = committedByTid
        val filesCapt          = filesList

        futures.add(submitTask {
          committedPathOpt match {
            case None =>
              recoverPathA(dirCapture, fs, tid, startedPath,
                pendingPaths, filesCapt, rootPath)
            case Some(committedPath) =>
              recoverPathB(dirCapture, fs, tid, startedPath,
                committedPath, allCommittedCapt, rootPath)
          }
        })
      }

      // Enqueue non-hidden subdirs for BFS continuation
      subDirs
        .withFilter(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
        .foreach(s => bfsQueue.add(s.getPath))
    }

    // Drain all recovery futures
    futures.asScala.foreach { f =>
      try { f.get(30, TimeUnit.MINUTES) }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover: task failed: ${e.getMessage}") }
    }
  }

  private def recoverPathA(
                            dir:          Path,
                            fs:           FileSystem,
                            tid:          String,
                            startedPath:  Path,
                            pendingPaths: Seq[Path],
                            filesInDir:   Seq[FileStatus],
                            rootDir:      Path): Unit = {

    logInfo(s"ManifestCommitProtocol.recover[A]: uncommitted write at $dir tid=$tid")

    // Source 1: _pending_<tid>_* pendingFiles (authoritative — written after rename)
    val fromPending: Set[String] = pendingPaths.flatMap { pp =>
      try { parsePendingFile(pp, fs).pendingFiles }
      catch { case _: Exception => Seq.empty[String] }
    }.toSet

    // Source 2: _started_ pendingFiles (backup — written before rename)
    // Covers crash window: rename done, _pending_ not yet written
    val fromStarted: Set[String] =
    try { parseStartedFile(startedPath, fs).pendingFiles.toSet }
    catch { case _: Exception => Set.empty[String] }

    // Source 3: filename tid-pattern scan (last resort — no manifest needed)
    val tidPattern = s"-tid-$tid-"
    val fromScan: Set[String] = filesInDir
      .withFilter(s => isDataFile(s.getPath.getName) &&
        s.getPath.getName.contains(tidPattern))
      .map(_.getPath.getName)
      .toSet

    val toDelete = fromPending ++ fromStarted ++ fromScan
    val deleted  = new AtomicInteger(0)

    val deleteTasks: Seq[() => Unit] = toDelete.toSeq.map { name => () =>
      val p = new Path(dir, name)
      try {
        if (fs.exists(p)) {
          fs.delete(p, false); deleted.incrementAndGet()
          logInfo("")
        }
      } catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[A]: cannot delete $p: ${e.getMessage}") }
    }
    parallelExec(deleteTasks, s"recoverA-del[$dir]")

    // Clean up _pending_ files then _started_
    pendingPaths.foreach(pp => safeDelete(fs, pp))
    safeDelete(fs, startedPath)

    logInfo(
      s"ManifestCommitProtocol.recover[A]: complete at $dir tid=$tid " +
        s"deleted=${deleted.get}/${toDelete.size} " +
        s"(pending=${fromPending.size} started=${fromStarted.size} scan=${fromScan.size})")
  }

  private def recoverPathB(
                            dir:           Path,
                            fs:            FileSystem,
                            tid:           String,
                            startedPath:   Path,
                            committedPath: Path,
                            allCommitted:  Map[String, Path],
                            rootDir:       Path): Unit = {

    logInfo(s"ManifestCommitProtocol.recover[B]: deferred delete at $dir tid=$tid")

    val manifest =
      try { parseCommittedFile(committedPath, fs) }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[B]: cannot parse $committedPath: ${e.getMessage}")
        return }

    val deleted = new AtomicInteger(0)
    val deleteTasks: Seq[() => Unit] = manifest.removedFiles
      .withFilter(isDataFile)
      .map { name => () =>
        val p = new Path(dir, name)
        try {
          if (fs.exists(p)) {
            fs.delete(p, false)
            deleted.incrementAndGet()
            logInfo("")
          }
        } catch { case e: Exception =>
          logWarning(
            s"ManifestCommitProtocol.recover[B]: cannot delete $p: ${e.getMessage}") }
      }
    parallelExec(deleteTasks, s"recoverB-del[$dir]")

    // If no data remains and no newer _committed_ from another job → delete dir
    val remainingData =
      try { fs.listStatus(dir).exists(s => isDataFile(s.getPath.getName)) }
      catch { case _: Exception => true }

    if (!remainingData && !allCommitted.keys.exists(_ != tid)) {
      try {
        if (fs.exists(dir)) {
          fs.delete(dir, true)
          logInfo(s"ManifestCommitProtocol.recover[B]: deleted empty stale $dir")
          cleanupEmptyAncestorsBFS(fs, dir.getParent, rootDir)
        }
      } catch { case e: Exception =>
        logWarning(
          s"ManifestCommitProtocol.recover[B]: cannot delete stale $dir: ${e.getMessage}") }
      return   // dir gone — skip deleting _started_
    }

    safeDelete(fs, startedPath)
    logInfo(
      s"ManifestCommitProtocol.recover[B]: complete at $dir tid=$tid " +
        s"deleted=${deleted.get}/${manifest.removedFiles.size}")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  ORPHAN CLEANUP — iterative BFS
  //
  //  Run AFTER recovery so it does not accidentally remove the _started_ files
  //  that PATH A needs to read.  Only removes _started_ files that have no
  //  _committed_, no _pending_ files (truly orphaned from very old jobs).
  // ═══════════════════════════════════════════════════════════════════════════

  private def cleanupOrphanedStartedBFS(dir: Path, fs: FileSystem): Unit = {
    val queue = new java.util.ArrayDeque[Path]()
    queue.add(dir)

    while (!queue.isEmpty) {
      val current  = queue.poll()
      val statuses = try { fs.listStatus(current) }
      catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      val committedTids: Set[String] = files
        .withFilter(_.getPath.getName.startsWith("_committed_"))
        .map(_.getPath.getName.stripPrefix("_committed_")).toSet

      val pendingTids: Set[String] = files
        .withFilter(_.getPath.getName.startsWith("_pending_"))
        .flatMap { s =>
          val parts = s.getPath.getName.stripPrefix("_pending_").split("_")
          if (parts.length >= 1) Some(parts(0)) else None
        }.toSet

      files
        .withFilter(_.getPath.getName.startsWith("_started_"))
        .foreach { s =>
          val tid = s.getPath.getName.stripPrefix("_started_")
          if (!committedTids.contains(tid) && !pendingTids.contains(tid)) {
            safeDelete(fs, s.getPath)
            logInfo(s"ManifestCommitProtocol: removed orphaned ${s.getPath}")
          }
        }

      subDirs
        .withFilter(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)

    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)

    deleteParallelism = jobContext.getConfiguration
      .getInt("spark.sql.manifest.deleteParallelism", 16)
    writeParallelism = jobContext.getConfiguration
      .getInt("spark.sql.manifest.writeParallelism", 16)

    val poolSize = math.max(deleteParallelism, writeParallelism)

    // Single shared pool for the entire job (manifests + deletes + listings)
    sharedPool = Executors.newFixedThreadPool(
      poolSize,
      new ThreadFactory {
        val counter = new AtomicInteger(0)
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"manifest-commit-${counter.incrementAndGet()}")
          t.setDaemon(true)
          t
        }
      })

    // Recovery MUST run before cleanupOrphanedStarted so PATH A can read _started_
    recoverBFS(outputDir, jobFs)

    // Now clean up truly orphaned _started_ (no _committed_, no _pending_)
    cleanupOrphanedStartedBFS(outputDir, jobFs)

    // Write root-level job lock
    writeStartedFile(jobFs, new Path(outputDir, s"_started_$jobId"), outputPath)

    logInfo(
      s"ManifestCommitProtocol.setupJob: path=$outputPath jobId=$jobId " +
        s"dynamic=$dynamicPartitionOverwrite poolSize=$poolSize")
  }

  /**
   * Called by InsertIntoHadoopFsRelationCommand for STATIC overwrite only
   * (skipped when dynamicPartitionOverwrite=true).
   *
   * Defers deletion: records dir paths only (not filenames) to avoid driver OOM.
   * Old filenames are listed lazily in commitJob step 2.
   */
  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    collectOldDirsBFS(fs, path)
    logInfo(
      s"ManifestCommitProtocol.deleteWithJob: deferred $path → " +
        s"${pendingDeleteDirs.size()} partition dirs recorded")
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    val rootPath = new Path(outputPath)

    // ── Step 1: unwrap CombinedCommitPayload from each task ──────────────────
    val mergedPartitionFiles =
      new java.util.concurrent.ConcurrentHashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayloadV6 =>
          cp.partitionFiles.forEach { (pDir, files) =>
            mergedPartitionFiles
              .compute(pDir, (_, existing) => {
                val list = if (existing == null) new java.util.ArrayList[String]() else existing
                list.addAll(files)
                list
              })
          }
          new TaskCommitMessage(cp.superObj)
        case _ => msg
      }
    }

    // ── Step 2: capture REMOVED lists BEFORE super.commitJob() ──────────────
    //
    // DYNAMIC: list old files NOW — super.commitJob() deletes these dirs next.
    // STATIC:  re-list dirs recorded in deleteWithJob (files still there).
    // Parallel listing for throughput.
    val removedByPartition =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.List[String]]()

    val listingDirs: Seq[String] =
      if (dynamicPartitionOverwrite) mergedPartitionFiles.keySet().asScala.toSeq
      else pendingDeleteDirs.asScala.toSeq

    val listTasks: Seq[() => Unit] = listingDirs.map { pDir => () =>
      val p      = new Path(pDir)
      val partFs = p.getFileSystem(jobContext.getConfiguration)
      val old    = listDataFileNames(partFs, partFs.makeQualified(p))
      if (old.nonEmpty)
        removedByPartition.put(pDir, old.toList.asJava)
      logInfo("")
    }
    parallelExec(listTasks, "step2-listOld")

    // ── Step 3: super.commitJob() ────────────────────────────────────────────
    //
    // DYNAMIC: fs.delete(old partition dirs) then fs.rename(staging → final)
    // STATIC:  fs.rename(staged files → final). Old files still coexist.
    // _committed_ MUST be written AFTER this call in both cases.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: PARALLEL write per-partition _committed_ (AFTER super) ──────
    //
    // After super: new files are at final location in all cases.
    // _committed_ is now safe to write.
    val writeCommittedTasks: Seq[() => Unit] =
    mergedPartitionFiles.asScala.toSeq.map { case (pDir, addedFiles) => () =>
      val partPath = new Path(pDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      val removed  = removedByPartition.getOrDefault(pDir, java.util.Collections.emptyList())
      writePartitionCommittedFile(
        partFs, partPath,
        addedFiles.asScala.toSeq,
        removed.asScala.toSeq)
    }
    parallelExec(writeCommittedTasks, "step4-writeCommitted")

    // ── Step 5: write root-level _committed_ ────────────────────────────────
    val allRemoved: Seq[String] =
      removedByPartition.values().asScala.flatMap(_.asScala).toSeq

    writeRootCommittedFile(
      jobFs, outputDir,
      new java.util.HashMap(mergedPartitionFiles),
      allRemoved)

    // ── Step 6: PARALLEL delete old data (STATIC overwrite only) ────────────
    //
    // Two cases:
    //   A. Partition HAS new data: delete old files by name only.
    //      Dir has new data + _committed_ — never delete the whole dir.
    //   B. Stale partition (no new data): delete entire dir recursively.
    //      Then clean empty ancestor dirs iteratively.
    if (!dynamicPartitionOverwrite) {

      // Separate written vs stale
      val fileDeleteTargets = new java.util.concurrent.ConcurrentLinkedQueue[(FileSystem, Path)]()
      val staleDirs         = mutable.ArrayBuffer.empty[(FileSystem, Path)]

      pendingDeleteDirs.asScala.foreach { qualDir =>
        val dirPath = new Path(qualDir)
        val dirFs   = dirPath.getFileSystem(jobContext.getConfiguration)

        if (mergedPartitionFiles.containsKey(qualDir)) {
          // Case A: written partition — delete old filenames only
          removedByPartition
            .getOrDefault(qualDir, java.util.Collections.emptyList())
            .forEach { name => fileDeleteTargets.add((dirFs, new Path(dirPath, name))) }
        } else {
          // Case B: stale partition — full dir delete
          staleDirs += ((dirFs, dirPath))
        }
      }

      // Parallel delete individual old files
      val fileDeleteCount = new AtomicLong(0)
      val fileTasks: Seq[() => Unit] = fileDeleteTargets.asScala.toSeq.map {
        case (fs, path) => () =>
          try {
            if (fs.exists(path)) {
              fs.delete(path, false); fileDeleteCount.incrementAndGet()
              logInfo("")
            }
          } catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: cannot delete $path: ${e.getMessage}") }
      }
      parallelExec(fileTasks, "step6-fileDelete")
      logInfo(
        s"ManifestCommitProtocol.commitJob step6: deleted " +
          s"${fileDeleteCount.get}/${fileDeleteTargets.size()} old data files")

      // Parallel delete stale partition dirs
      val dirDeleteCount = new AtomicLong(0)
      val dirTasks: Seq[() => Unit] = staleDirs.toSeq.map {
        case (fs, path) => () =>
          try {
            if (fs.exists(path)) {
              fs.delete(path, true); dirDeleteCount.incrementAndGet()
              logInfo("")
            }
          } catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: cannot delete stale dir $path: ${e.getMessage}") }
      }
      parallelExec(dirTasks, "step6-staleDirDelete")
      logInfo(
        s"ManifestCommitProtocol.commitJob step6: deleted " +
          s"${dirDeleteCount.get}/${staleDirs.size} stale partition dirs")

      // Clean empty ancestors (serial — small number of dirs, parent scan cheap)
      staleDirs.foreach { case (fs, dirPath) =>
        cleanupEmptyAncestorsBFS(fs, dirPath.getParent, rootPath)
      }

      pendingDeleteDirs.clear()
    }

    // ── Step 7: PARALLEL delete _started_ + _pending_ (transaction closed) ──
    val cleanupTargets =
      new java.util.concurrent.ConcurrentLinkedQueue[(FileSystem, Path)]()

    // Root _started_ always deleted
    cleanupTargets.add((jobFs, new Path(outputDir, s"_started_$jobId")))

    if (dynamicPartitionOverwrite) {
      // Dynamic: super deleted old partition dirs entirely.
      // Partition-level _started_ gone with them.
      // Clean residual old manifests in newly renamed dirs.
      val cleanOldTasks: Seq[() => Unit] =
      mergedPartitionFiles.keySet().asScala.toSeq.map { pDir => () =>
        val p   = new Path(pDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifestsInDir(p, pFs)
      }
      parallelExec(cleanOldTasks, "step7-cleanOldManifests")

    } else {
      // Static: collect _started_ + all _pending_<jobId>_* from written partitions
      // (stale partition dirs were deleted entirely in step 6)
      val collectTasks: Seq[() => Unit] =
      mergedPartitionFiles.keySet().asScala.toSeq.map { pDir => () =>
        val p   = new Path(pDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)

        cleanupTargets.add((pFs, new Path(p, s"_started_$jobId")))

        try {
          pFs.listStatus(p)
            .withFilter(_.getPath.getName.startsWith(s"_pending_${jobId}_"))
            .foreach(s => cleanupTargets.add((pFs, s.getPath)))
        } catch { case _: Exception => }
      }
      parallelExec(collectTasks, "step7-collectCleanup")
    }

    val cleanTasks: Seq[() => Unit] =
      cleanupTargets.asScala.toSeq.map { case (fs, path) => () => safeDelete(fs, path) }
    parallelExec(cleanTasks, "step7-cleanup")

    // Shutdown pool — all work complete
    sharedPool.shutdown()
    sharedPool.awaitTermination(5, TimeUnit.MINUTES)

    logInfo(
      s"ManifestCommitProtocol.commitJob: COMPLETE " +
        s"path=$outputPath dynamic=$dynamicPartitionOverwrite " +
        s"partitions_written=${mergedPartitionFiles.size()} " +
        s"total_added=${mergedPartitionFiles.values().asScala.map(_.size()).sum} " +
        s"total_removed=${allRemoved.size}")
  }

  override def abortJob(jobContext: JobContext): Unit = {
    pendingDeleteDirs.clear()
    try {
      if (sharedPool != null && !sharedPool.isShutdown) sharedPool.shutdownNow()
    } catch { case _: Exception => }
    super.abortJob(jobContext)
    // Leave _started_ in place — signals failed write to ManifestAwareFileIndex
    logInfo(s"ManifestCommitProtocol.abortJob: old data preserved at $outputPath")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  //  Object arrives here after Java deserialization — @transient fields = null.
  //  setupTask() is the ONLY safe place to initialize executor-side state.
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  /**
   * Spark 3.5 calls the FileNameSpec variant ONLY.
   * Overriding the deprecated (ext: String) variant does nothing — partition
   * _started_ would never be written and taskPartitionFiles would stay empty.
   */
  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir:         Option[String],
                                spec:        FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFile(taskContext, dir, spec)

    // Compute QUALIFIED final partition dir — must match pendingDeleteDirs keys
    val rawPartDir = dir match {
      case Some(d) => new Path(outputPath, d)
      case None    => new Path(outputPath)
    }
    val partFs  = rawPartDir.getFileSystem(taskContext.getConfiguration)
    val partDir = partFs.makeQualified(rawPartDir).toString

    // Write partition-level _started_ (lock signal) once per partition per task
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      writeStartedFile(partFs, new Path(partPath, s"_started_$jobId"),
        partitionPath = partDir,
        pendingFiles  = Seq.empty)   // pendingFiles filled in at commitTask
      logDebug(
        s"ManifestCommitProtocol: wrote _started_(empty) at $partDir " +
          s"task=${taskContext.getTaskAttemptID}")
    }

    taskPartitionFiles
      .computeIfAbsent(partDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)

    stagingPath
  }

  override def newTaskTempFileAbsPath(
                                       taskContext:  TaskAttemptContext,
                                       absoluteDir:  String,
                                       spec:         FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    val raw         = new Path(absoluteDir)
    val qualDir     = raw.getFileSystem(taskContext.getConfiguration)
      .makeQualified(raw).toString
    taskPartitionFiles
      .computeIfAbsent(qualDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)
    stagingPath
  }

  /**
   * Three-phase commitTask:
   *
   *   Phase 1 — update partition _started_ with actual file list (BEFORE rename).
   *             If cluster crashes after rename but before _pending_ is written,
   *             recovery PATH A reads _started_ and finds every file to delete.
   *
   *   Phase 2 — super.commitTask(): staging → final rename.
   *             Files are now at final location on disk.
   *
   *   Phase 3 — write _pending_<jobId>_<taskAttemptId>: authoritative per-task record.
   *             One file per task; never overwritten by other tasks.
   *             Recovery PATH A unions all _pending_ files to get complete list.
   */
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    val taskAttemptId = taskContext.getTaskAttemptID.toString

    // Snapshot partition → files for this task
    val snapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (pDir, files) =>
      snapshot.put(pDir, new java.util.ArrayList[String](files))
    }

    // ── Phase 1: update _started_ with file list BEFORE rename ──────────────
    snapshot.forEach { (pDir, files) =>
      updatePartitionStarted(pDir, files.asScala.toSeq, taskContext)
    }

    // ── Phase 2: staging → final rename ─────────────────────────────────────
    val superMsg = super.commitTask(taskContext)
    // Files are now at final location.

    // ── Phase 3: write _pending_<jobId>_<taskAttemptId> per partition ────────
    snapshot.forEach { (pDir, files) =>
      val partPath = new Path(pDir)
      val partFs   = partPath.getFileSystem(taskContext.getConfiguration)
      writePendingFile(partFs, partPath, taskAttemptId, files.asScala.toSeq)
      logDebug(
        s"ManifestCommitProtocol: wrote _pending_ ${files.size()} files " +
          s"at $pDir task=$taskAttemptId")
    }

    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    new TaskCommitMessage(CombinedCommitPayloadV6(superMsg.obj, snapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
    // Leave _started_ and _pending_ — recovery PATH A cleans them next setupJob
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  UTILITIES
  // ═══════════════════════════════════════════════════════════════════════════

  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning(
        s"ManifestCommitProtocol: executor state null — calling setupTask() " +
          s"task=${taskContext.getTaskAttemptID}")
      setupTask(taskContext)
    }
  }

  private def cleanupOldManifestsInDir(dir: Path, fs: FileSystem): Unit =
    try {
      fs.listStatus(dir)
        .withFilter { s =>
          val n = s.getPath.getName
          (n.startsWith("_committed_") || n.startsWith("_started_") ||
            n.startsWith("_pending_")) &&
            !n.endsWith(jobId) && !n.contains(s"_${jobId}_")
        }
        .foreach(s => safeDelete(fs, s.getPath))
    } catch { case _: Exception => }
}


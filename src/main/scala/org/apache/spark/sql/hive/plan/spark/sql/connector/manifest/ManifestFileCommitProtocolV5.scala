package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.{TaskCommitMessage}
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import java.time.Instant
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
//  Top-level types — outside the class so pattern match survives serialization
// ─────────────────────────────────────────────────────────────────────────────

/** Carries super's Tuple2 + our partition→files map in one TaskCommitMessage. */
final case class CombinedCommitPayloadV5(
                                        superObj:       Any,
                                        partitionFiles: java.util.HashMap[String, java.util.List[String]]
                                      )

/** Parsed content of a _started_<tid> file (job-level lock signal). */
final case class StartedManifestV5(
                                  jobId:         String,
                                  partitionPath: String,
                                  startedAt:     String,
                                  outputPath:    String,
                                  dynamic:       Boolean
                                )

/**
 * Per-task pending file record: _pending_<jobId>_<taskAttemptId>
 * One file per task per partition dir.  Lists exactly the data files
 * that task wrote and that are uncommitted at the job level.
 * Used by PATH A recovery to identify and delete garbage files precisely,
 * even when multiple tasks wrote to the same partition.
 */
final case class PendingTaskManifestV5
(
                                      jobId:         String,
                                      taskAttemptId: String,
                                      partitionPath: String,
                                      startedAt:     String,
                                      pendingFiles:  Seq[String]
                                    )

/** Parsed content of _committed_<tid>. */
final case class CommittedManifestV5(
                                    tid:          String,
                                    addedFiles:   Seq[String],
                                    removedFiles: Seq[String]
                                  )

// ─────────────────────────────────────────────────────────────────────────────
//  ManifestCommitProtocol — petabyte-scale, DBIO-compatible
//
//  FILE LAYOUT per partition dir:
//    _started_<jobId>                  → job-level lock (minimal content)
//    _pending_<jobId>_<taskAttemptId>  → per-task uncommitted files (one per task)
//    _committed_<jobId>                → committed file list (added + removed)
//
//  Why _pending_ instead of storing in _started_?
//    Multiple tasks can write to the same partition. Each needs its own
//    file so they don't overwrite each other's pending file list.
//    Recovery PATH A unions ALL _pending_<jobId>_* files to get the
//    complete set of uncommitted files for that partition.
//
//  RECOVERY (next setupJob):
//    PATH A  _started_ exists, _committed_ absent  → delete all _pending_ files
//    PATH B  _started_ + _committed_ both exist    → complete deferred deletes
//
//  PARALLEL OPERATIONS (shared pool, created once per job):
//    Manifest writes, file deletes, partition scans all use one pool.
//    Rate-limited to respect object store API quotas.
// ─────────────────────────────────────────────────────────────────────────────

class ManifestFileCommitProtocolV5(
                              jobId: String,
                              outputPath: String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ═══════════════════════════════════════════════════════════════════════════
  //  SERIALIZATION CONTRACT
  //
  //  @transient fields = null after Java deserialization to executor.
  //  DRIVER-side  → initialize in setupJob()
  //  EXECUTOR-side → initialize in setupTask()  ← NEVER inline
  // ═══════════════════════════════════════════════════════════════════════════

  // ── DRIVER-SIDE ─────────────────────────────────────────────────────────────
  @transient private var outputDir:     Path         = _
  @transient private var jobFs:         FileSystem   = _
  @transient private var sharedPool:    ExecutorService = _  // ONE pool per job
  @transient private var deleteParallelism: Int      = 8    // set from conf in setupJob
  @transient private var maxFilesInMemory: Int       = 100000 // OOM guard

  // FIX #5: store only DIRECTORY PATHS (not filenames) to avoid driver OOM.
  // Old filenames are re-listed lazily at step 6 (dirs are still there because
  // deleteWithJob deferred the actual deletion).
  @transient private val pendingDeleteDirs =
  mutable.LinkedHashSet.empty[String]   // qualified absolute dir paths

  // ── EXECUTOR-SIDE ── initialized in setupTask(), NEVER inline ───────────────
  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String, java.util.concurrent.CopyOnWriteArrayList[String]] = _

  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ═══════════════════════════════════════════════════════════════════════════
  //  HELPERS
  // ═══════════════════════════════════════════════════════════════════════════

  @inline private def isDataFile(name: String): Boolean =
    name.nonEmpty && !name.startsWith("_") && !name.startsWith(".")

  private def listDataFileNames(fs: FileSystem, dir: Path): Seq[String] =
    try {
      fs.listStatus(dir)
        .filter(s => !s.isDirectory && isDataFile(s.getPath.getName))
        .map(_.getPath.getName)
        .toSeq
    } catch { case _: Exception => Seq.empty }

  /** Submits work to shared pool, returns Future. */
  private def submit[T](body: => T): java.util.concurrent.Future[T] =
    sharedPool.submit(new Callable[T] { def call(): T = body })

  /** Runs all callables in parallel, collects results, logs errors. */
  private def parallelRun[T](
                              tasks:  Seq[() => T],
                              label:  String): Seq[T] = {

    if (tasks.isEmpty) return Seq.empty
    val futures = tasks.map(t => submit(t()))
    futures.flatMap { f =>
      try { Some(f.get()) }
      catch {
        case e: Exception =>
          logWarning(s"ManifestCommitProtocol[$label]: task failed: ${e.getMessage}")
          None
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _started_ — JOB-LEVEL LOCK (minimal content, no pendingFiles)
  // ═══════════════════════════════════════════════════════════════════════════

  private def writeStartedFile(
                                fs:            FileSystem,
                                path:          Path,
                                partitionPath: String): Unit = {

    val content =
      s"""{
         |  "jobId": "$jobId",
         |  "startedAt": "${Instant.now()}",
         |  "outputPath": "$outputPath",
         |  "partitionPath": "$partitionPath",
         |  "dynamic": $dynamicPartitionOverwrite
         |}""".stripMargin
    writeJson(fs, path, content)
  }

  private def parseStartedFile(path: Path, fs: FileSystem): StartedManifestV5 =
    try {
      val c = readFileContent(path, fs, 1024 * 1024)
      StartedManifestV5(
        jobId         = extractJsonString(c, "jobId").getOrElse(jobId),
        partitionPath = extractJsonString(c, "partitionPath").getOrElse(path.getParent.toString),
        startedAt     = extractJsonString(c, "startedAt").getOrElse(""),
        outputPath    = extractJsonString(c, "outputPath").getOrElse(outputPath),
        dynamic       = c.contains("\"dynamic\": true"))
    } catch {
      case e: Exception =>
        logWarning(s"ManifestCommitProtocol: cannot parse _started_ $path: ${e.getMessage}")
        StartedManifestV5(jobId, path.getParent.toString, "", outputPath, false)
    }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _pending_<jobId>_<taskAttemptId> — PER-TASK UNCOMMITTED FILES
  //
  //  FIX #3: one file per task, never overwritten by another task.
  //  Recovery unions all _pending_<jobId>_* to get complete pending file set.
  // ═══════════════════════════════════════════════════════════════════════════

  private def pendingFileName(taskAttemptId: String): String =
    s"_pending_${jobId}_$taskAttemptId"

  private def writePendingFile(
                                fs:            FileSystem,
                                partDir:       Path,
                                taskAttemptId: String,
                                files:         Seq[String]): Unit = {

    val filesJson = files.map(f => s""""$f"""").mkString(",")
    val content =
      s"""{
         |  "jobId": "$jobId",
         |  "taskAttemptId": "$taskAttemptId",
         |  "partitionPath": "${partDir.toString}",
         |  "startedAt": "${Instant.now()}",
         |  "pendingFiles": [$filesJson]
         |}""".stripMargin
    writeJson(fs, new Path(partDir, pendingFileName(taskAttemptId)), content)
  }

  private def parsePendingFile(path: Path, fs: FileSystem): PendingTaskManifestV5 =
    try {
      val c = readFileContent(path, fs, 64 * 1024 * 1024)
      PendingTaskManifestV5(
        jobId         = extractJsonString(c, "jobId").getOrElse(jobId),
        taskAttemptId = extractJsonString(c, "taskAttemptId").getOrElse(""),
        partitionPath = extractJsonString(c, "partitionPath").getOrElse(path.getParent.toString),
        startedAt     = extractJsonString(c, "startedAt").getOrElse(""),
        pendingFiles  = extractJsonStringArray(c, "pendingFiles"))
    } catch {
      case e: Exception =>
        logWarning(s"ManifestCommitProtocol: cannot parse _pending_ $path: ${e.getMessage}")
        PendingTaskManifestV5(jobId, "", path.getParent.toString, "", Seq.empty)
    }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _committed_ — JOB-COMMITTED FILE LIST
  // ═══════════════════════════════════════════════════════════════════════════

  private def writeCommittedFile(
                                  fs:      FileSystem,
                                  dir:     Path,
                                  added:   Seq[String],
                                  removed: Seq[String]): Unit = {

    val addedJson   = added.map(f => s""""$f"""").mkString(",")
    val removedJson = removed.map(f => s""""$f"""").mkString(",")
    writeJson(
      fs,
      new Path(dir, s"_committed_$jobId"),
      s"""{"added":[$addedJson],"removed":[$removedJson]}""")
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

  private def parseCommittedFile(path: Path, fs: FileSystem): CommittedManifestV5 = {
    val c = readFileContent(path, fs, 256 * 1024 * 1024)
    CommittedManifestV5(
      tid          = path.getName.stripPrefix("_committed_"),
      addedFiles   = extractJsonStringArray(c, "added"),
      removedFiles = extractJsonStringArray(c, "removed"))
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  ITERATIVE BFS TRAVERSAL  (FIX #2 — replaces recursive methods)
  //
  //  All directory traversal now uses an explicit queue — no JVM stack risk
  //  regardless of partition depth (year/month/day/hour/region/...).
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * FIX #2 + #5: Iterative BFS collecting only DIRECTORY PATHS that directly
   * contain data files. Stores paths, not filenames — avoids driver OOM.
   */
  private def collectOldDirsIterative(fs: FileSystem, startDir: Path): Unit = {
    val queue = new java.util.ArrayDeque[Path]()
    queue.add(startDir)

    while (!queue.isEmpty) {
      val dir      = queue.poll()
      val statuses = try { fs.listStatus(dir) } catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      val hasDataFiles = files.exists(s => isDataFile(s.getPath.getName))
      if (hasDataFiles) {
        pendingDeleteDirs += fs.makeQualified(dir).toString
        logDebug(s"ManifestCommitProtocol.collectOldDirs: $dir")
      }

      subDirs
        .filterNot(s => { val n = s.getPath.getName; n.startsWith("_") || n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }
  }

  /**
   * FIX #2: Iterative upward walk deleting empty ancestor dirs up to stopAt.
   */
  private def cleanupEmptyAncestorsIterative(
                                              fs:      FileSystem,
                                              startAt: Path,
                                              stopAt:  Path): Unit = {

    val qualStopAt = try { fs.makeQualified(stopAt) } catch { case _: Exception => return }
    var current    = try { fs.makeQualified(startAt) } catch { case _: Exception => return }

    while (current != qualStopAt && current.toString.startsWith(qualStopAt.toString)) {
      try {
        if (!fs.exists(current)) {
          current = current.getParent
        } else {
          val hasRealContent = fs.listStatus(current)
            .exists(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })

          if (!hasRealContent) {
            fs.delete(current, true)
            logInfo(s"ManifestCommitProtocol: deleted empty ancestor $current")
            current = current.getParent
          } else {
            return  // has real siblings — stop climbing
          }
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
  //  RECOVERY — iterative BFS, runs BEFORE the new write
  //
  //  FIX #2: Iterative, not recursive.
  //  FIX #3: Reads _pending_<jobId>_* files for PATH A.
  //  FIX #8: Runs AFTER the new _started_ is written (not before) — wait, actually
  //          recovery must run BEFORE the new write. But cleanupOrphanedStarted
  //          must run AFTER recovery so it doesn't eat recovery's _started_ files.
  //
  //  PATH A: _started_<tid> exists, no _committed_<tid>
  //    → scan all _pending_<tid>_* in this dir → union pendingFiles → delete them
  //    → delete all _pending_<tid>_* files
  //    → delete _started_<tid>
  //
  //  PATH B: _started_<tid> AND _committed_<tid> both exist
  //    → read _committed_.removed[] → delete each file still present
  //    → if dir empty after → delete dir + empty ancestors
  //    → delete _started_<tid>
  // ═══════════════════════════════════════════════════════════════════════════

  private def recoverIterative(rootDir: Path, fs: FileSystem): Unit = {
    val pool      = sharedPool
    val queue     = new java.util.concurrent.LinkedBlockingQueue[Path]()
    val rootPath  = new Path(outputPath)
    val futures   = mutable.ArrayBuffer[java.util.concurrent.Future[_]]()

    queue.add(rootDir)

    // BFS — enqueue dirs, submit recovery for each dir in parallel
    while (!queue.isEmpty) {
      val dir = queue.poll()
      val statuses = try { fs.listStatus(dir) }
      catch { case _: Exception => Array.empty[FileStatus] }

      val (subDirs, files) = statuses.partition(_.isDirectory)

      // Collect manifest files in this dir
      val committedByTid: Map[String, Path] = files
        .filter(_.getPath.getName.startsWith("_committed_"))
        .map(s => s.getPath.getName.stripPrefix("_committed_") -> s.getPath).toMap

      val startedByTid: Map[String, Path] = files
        .filter(_.getPath.getName.startsWith("_started_"))
        .map(s => s.getPath.getName.stripPrefix("_started_") -> s.getPath).toMap

      // Collect _pending_<tid>_* grouped by tid
      val pendingByTid: Map[String, Seq[Path]] = files
        .filter { s =>
          val n = s.getPath.getName
          n.startsWith("_pending_") && n.split("_").length >= 3
        }
        .groupBy { s =>
          val parts = s.getPath.getName.stripPrefix("_pending_").split("_")
          // _pending_<jobId>_<taskAttemptId> → tid = jobId portion
          if (parts.length >= 2) parts(0) else ""
        }
        .filter(_._1.nonEmpty)
        .mapValues(_.map(_.getPath).toSeq)

      // Submit recovery work for each tid that has a _started_ in this dir
      startedByTid.foreach { case (tid, startedPath) =>
        val dirCapture = dir
        committedByTid.get(tid) match {

          // PATH A: _started_ without _committed_
          case None =>
            val pendingPaths = pendingByTid.getOrElse(tid, Seq.empty)
            futures += pool.submit(new Runnable {
              def run(): Unit = recoverPathA(dirCapture, fs, tid, startedPath, pendingPaths, rootPath)
            })

          // PATH B: _started_ + _committed_
          case Some(committedPath) =>
            futures += pool.submit(new Runnable {
              def run(): Unit = recoverPathB(dirCapture, fs, tid, startedPath, committedPath, committedByTid, rootPath)
            })
        }
      }

      // Enqueue non-hidden subdirs for BFS continuation
      subDirs
        .filterNot(s => { val n = s.getPath.getName; n.startsWith("_") || n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }

    // Wait for all recovery tasks
    futures.foreach { f =>
      try { f.get(10, TimeUnit.MINUTES) }
      catch { case e: Exception => logWarning(s"ManifestCommitProtocol.recover: task failed: ${e.getMessage}") }
    }
  }

  private def recoverPathA(
                            dir:          Path,
                            fs:           FileSystem,
                            tid:          String,
                            startedPath:  Path,
                            pendingPaths: Seq[Path],
                            rootDir:      Path): Unit = {

    logInfo(s"ManifestCommitProtocol.recover[A]: uncommitted write at $dir tid=$tid")

    // Union pendingFiles from all _pending_<tid>_* files in this dir
    val allPendingFiles: Set[String] = pendingPaths.flatMap { pp =>
      try { parsePendingFile(pp, fs).pendingFiles }
      catch { case _: Exception => Seq.empty }
    }.toSet

    // Also scan for files whose names contain the tid pattern (fallback for
    // tasks that crashed before writing their _pending_ file)
    val tidPattern = s"-tid-$tid-"
    val fromScan: Set[String] = try {
      fs.listStatus(dir)
        .filter(s => isDataFile(s.getPath.getName) && s.getPath.getName.contains(tidPattern))
        .map(_.getPath.getName)
        .toSet
    } catch { case _: Exception => Set.empty }

    val toDelete = allPendingFiles ++ fromScan
    val deleted  = new AtomicInteger(0)

    val tasks: Seq[() => Unit] = toDelete.toSeq.map { name => () =>
      val p = new Path(dir, name)
      try {
        if (fs.exists(p)) {
          fs.delete(p, false);
          deleted.incrementAndGet()
          logInfo(s"path ${p.getName} got deleted")
        }
      } catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[A]: cannot delete $p: ${e.getMessage}") }
    }
    parallelRun(tasks, s"recoverA[$dir]")

    // Delete _pending_ files and _started_
    pendingPaths.foreach(pp => safeDelete(fs, pp))
    safeDelete(fs, startedPath)

    logInfo(s"ManifestCommitProtocol.recover[A]: $dir tid=$tid deleted=${deleted.get}/${toDelete.size}")
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

    val manifest = try { parseCommittedFile(committedPath, fs) }
    catch { case e: Exception =>
      logWarning(s"ManifestCommitProtocol.recover[B]: cannot parse $committedPath: ${e.getMessage}")
      return }

    val deleted = new AtomicInteger(0)
    val tasks: Seq[() => Unit] = manifest.removedFiles.filter(isDataFile).map { name => () =>
      val p = new Path(dir, name)
      try {
        if (fs.exists(p)) {

          deleted.incrementAndGet()
          fs.delete(p, false);
          logInfo("got deleted")
        }else{
          logInfo("got deleted")
        }

      } catch {
        case e: Exception => {
          logError(s"ManifestCommitProtocol.recover[B]: cannot delete $p: ${e.getMessage}")
         // throw e
        }
      }
    }
    parallelRun(tasks, s"recoverB[$dir]")

    // If partition is now empty and has no other _committed_ from newer jobs → delete dir
    val remainingData = try {
      fs.listStatus(dir).exists(s => isDataFile(s.getPath.getName))
    } catch { case _: Exception => true }

    if (!remainingData && !allCommitted.keys.exists(_ != tid)) {
      try {
        if (fs.exists(dir)) {
          fs.delete(dir, true)
          logInfo(s"ManifestCommitProtocol.recover[B]: deleted empty stale dir $dir")
          cleanupEmptyAncestorsIterative(fs, dir.getParent, rootDir)
        }
      } catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[B]: cannot delete $dir: ${e.getMessage}") }
      return
    }

    safeDelete(fs, startedPath)
    logInfo(s"ManifestCommitProtocol.recover[B]: $dir tid=$tid deleted=${deleted.get}/${manifest.removedFiles.size}")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)

    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)

    // FIX #1: read conf here, store in var — not val
    deleteParallelism = jobContext.getConfiguration
      .getInt("spark.sql.manifest.deleteParallelism", 16)
    maxFilesInMemory = jobContext.getConfiguration
      .getInt("spark.sql.manifest.maxFilesInMemory", 100000)

    // FIX #4: create ONE shared thread pool for the entire job lifetime
    sharedPool = Executors.newFixedThreadPool(
      deleteParallelism,
      new ThreadFactory {
        val counter = new AtomicInteger(0)
        def newThread(r: Runnable): Thread = {
          val t = new Thread(r, s"manifest-commit-${counter.incrementAndGet()}")
          t.setDaemon(true)
          t
        }
      })

    // FIX #8: recovery runs BEFORE cleanupOrphanedStarted so recovery's
    // own _started_ files are not deleted before PATH A can read them.
    recoverIterative(outputDir, jobFs)

    // Now clean up any _started_ files that have no _committed_ and
    // no _pending_ files (truly orphaned from very old failed jobs)
    cleanupOrphanedStartedIterative(outputDir, jobFs)

    // Write root-level job lock
    writeStartedFile(jobFs, new Path(outputDir, s"_started_$jobId"), outputPath)

    logInfo(
      s"ManifestCommitProtocol.setupJob: path=$outputPath jobId=$jobId " +
        s"parallelism=$deleteParallelism dynamic=$dynamicPartitionOverwrite")
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    // FIX #5: store only directory paths, not filenames — avoids driver OOM.
    // Filenames are re-listed lazily in step 6 (dirs still contain old files
    // because deleteWithJob defers the actual deletion).
    collectOldDirsIterative(fs, path)
    logInfo(
      s"ManifestCommitProtocol.deleteWithJob: deferred $path → " +
        s"${pendingDeleteDirs.size} partition dirs recorded")
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    // ── Step 1: unwrap CombinedCommitPayload ────────────────────────────────
    val mergedPartitionFiles = new java.util.HashMap[String, java.util.List[String]]()
    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayloadV5 =>
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
    // DYNAMIC: list old files NOW before super deletes them.
    // STATIC:  re-list dirs recorded in deleteWithJob (FIX #5: lazy listing).
    val removedByPartition = new java.util.concurrent.ConcurrentHashMap[
      String, java.util.List[String]]()
    val newFileSet = mergedPartitionFiles.asScala.flatMap(f => f._2.asScala).toSet
    if (dynamicPartitionOverwrite) {
      // Parallel listing of old files in each partition being overwritten
      val listTasks: Seq[() => Unit] = mergedPartitionFiles.keySet().asScala.toSeq.map { partDir => () =>
        val p      = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        val old    = listDataFileNames(partFs, partFs.makeQualified(p)).filterNot(newFileSet.contains)
        if (old.nonEmpty) removedByPartition.put(partDir, old.asJava)

        println("inside dynamic partition overwrite")
      }
      parallelRun(listTasks, "step2-listOld")

    } else {
      // Static: re-list each recorded dir to get current old filenames.
      // This is lazy — avoids storing millions of filenames on driver.
      val listTasks: Seq[() => Unit] = pendingDeleteDirs.toSeq.map { qualDir => () =>
        val p      = new Path(qualDir)
        val dirFs  = p.getFileSystem(jobContext.getConfiguration)
        val old    = listDataFileNames(dirFs, p).filterNot(newFileSet.contains)
        if (old.nonEmpty) removedByPartition.put(qualDir, old.asJava)

        println("inside static partition overwrite")
      }
      parallelRun(listTasks, "step2-listOldStatic")
    }

    // ── Step 3: super.commitJob() ────────────────────────────────────────────
    // DYNAMIC: deletes old partition dirs, renames staging → final.
    // STATIC:  renames staged files to final (old files coexist until step 6).
    // _committed_ MUST be written AFTER this call.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: PARALLEL write per-partition _committed_ (AFTER super) ──────
    // FIX #6: parallel writes — one FS call per partition, all concurrent.
    val writeCommittedTasks: Seq[() => Unit] =
    mergedPartitionFiles.asScala.toSeq.map { case (partDir, addedFiles) => () =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      val removed  = removedByPartition.getOrDefault(
        partDir, new java.util.ArrayList[String]())
      writeCommittedFile(
        partFs, partPath,
        addedFiles.asScala.toSeq,
        removed.asScala.toSeq)
    }
    parallelRun(writeCommittedTasks, "step4-writeCommitted")

    // ── Step 5: write root-level _committed_ ────────────────────────────────
    val allRemoved = removedByPartition.values().asScala.flatMap(_.asScala).toSeq
    writeRootCommittedFile(jobFs, outputDir, mergedPartitionFiles, allRemoved)

    // ── Step 6: PARALLEL delete of old data ─────────────────────────────────
    if (!dynamicPartitionOverwrite) {

      // Separate into: written partitions (delete files) vs stale (delete dirs)
      val fileDeleteTargets  = new java.util.concurrent.ConcurrentLinkedQueue[(FileSystem, Path)]()
      val staleDirsToDelete  = mutable.ArrayBuffer[(FileSystem, Path)]()

      pendingDeleteDirs.foreach { qualDir =>
        val dirPath = new Path(qualDir)
        val dirFs   = dirPath.getFileSystem(jobContext.getConfiguration)

        if (mergedPartitionFiles.containsKey(qualDir)) {
          // Written partition: delete only old files (dir has new data + _committed_)
          removedByPartition.getOrDefault(qualDir, new java.util.ArrayList[String]())
            .forEach { name => fileDeleteTargets.add((dirFs, new Path(dirPath, name))) }
        } else {
          // Stale partition: entire dir must go
          staleDirsToDelete += ((dirFs, dirPath))
        }
      }

      // Parallel delete of old individual files
      val fileTasks: Seq[() => Boolean] = fileDeleteTargets.asScala.toSeq.map {
        case (fs, path) => () =>
          try { if (fs.exists(path)) { fs.delete(path, false); true } else false }
          catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: delete $path: ${e.getMessage}"); false }
      }
      val deletedFiles = parallelRun(fileTasks, "step6-files").count(identity)
      logInfo(
        s"ManifestCommitProtocol.commitJob: deleted $deletedFiles/${fileDeleteTargets.size()} " +
          s"old data files (parallelism=$deleteParallelism)")

      // Parallel delete of stale dirs
      val dirTasks: Seq[() => Boolean] = staleDirsToDelete.toSeq.map {
        case (fs, path) => () =>
          try { if (fs.exists(path)) { fs.delete(path, true); true } else false }
          catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: delete stale dir $path: ${e.getMessage}"); false }
      }
      val deletedDirs = parallelRun(dirTasks, "step6-staleDirs").count(identity)
      logInfo(
        s"ManifestCommitProtocol.commitJob: deleted $deletedDirs/${staleDirsToDelete.size} " +
          s"stale partition dirs")

      // Clean empty ancestors after stale dir deletions (serial — few ancestors)
      val rootPath = new Path(outputPath)
      staleDirsToDelete.foreach { case (fs, dirPath) =>
        cleanupEmptyAncestorsIterative(fs, dirPath.getParent, rootPath)
      }

      pendingDeleteDirs.clear()
    }

    // ── Step 7: PARALLEL delete of _pending_ + _started_ files ─────────────
    // FIX: parallel deletion of per-task _pending_ files and partition _started_ files

    // Parallel delete: all partition-level _started_ + _pending_ files
    val cleanupTargets = new mutable.ArrayBuffer[(FileSystem, Path)]()

    // Root _started_
    cleanupTargets += ((jobFs, new Path(outputDir, s"_started_$jobId")))

    if (dynamicPartitionOverwrite) {
      // Dynamic: super deleted old dirs in step 3. New dirs have no _started_.
      // Clean only old manifests from prior jobs in newly created partition dirs.
      val cleanOldTasks: Seq[() => Unit] = mergedPartitionFiles.keySet().asScala.toSeq.map { partDir => () =>
        val p   = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifestsInDir(p, pFs)
      }
      parallelRun(cleanOldTasks, "step7-cleanOldManifests")

    } else {
      // Static: delete partition-level _started_ + _pending_<jobId>_* files
      // for all WRITTEN partitions (stale partition dirs deleted entirely in step 6)
      mergedPartitionFiles.keySet().asScala.foreach { partDir =>
        val p   = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)

        cleanupTargets += ((pFs, new Path(p, s"_started_$jobId")))

        // Delete all _pending_<jobId>_* files in this partition
        try {
          pFs.listStatus(p)
            .filter { s =>
              val n = s.getPath.getName
              n.startsWith(s"_pending_${jobId}_")
            }
            .foreach(s => cleanupTargets += ((pFs, s.getPath)))
        } catch { case _: Exception => }
      }
    }

    val cleanTasks: Seq[() => Unit] = cleanupTargets.toSeq.map {
      case (fs, path) => () => safeDelete(fs, path)
    }
    parallelRun(cleanTasks, "step7-cleanup")

    // ── Shutdown shared pool ────────────────────────────────────────────────
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
      if (sharedPool != null && !sharedPool.isShutdown) {
        sharedPool.shutdownNow()
      }
    } catch { case _: Exception => }
    super.abortJob(jobContext)
    logInfo(s"ManifestCommitProtocol.abortJob: old data preserved at $outputPath")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  /** Spark 3.5 calls the FileNameSpec variant. Overriding the deprecated ext:String one does nothing. */
  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir: Option[String],
                                spec: FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFile(taskContext, dir, spec)

    val rawPartDir = dir match {
      case Some(d) => new Path(outputPath, d)
      case None    => new Path(outputPath)
    }
    val partFs  = rawPartDir.getFileSystem(taskContext.getConfiguration)
    val partDir = partFs.makeQualified(rawPartDir).toString

    // Write partition-level _started_ (lock signal) once per partition per task
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      writeStartedFile(partFs, new Path(partPath, s"_started_$jobId"), partDir)
      logDebug(
        s"ManifestCommitProtocol: wrote _started_ at $partDir " +
          s"task=${taskContext.getTaskAttemptID}")
    }

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
    val raw         = new Path(absoluteDir)
    val qualDir     = raw.getFileSystem(taskContext.getConfiguration)
      .makeQualified(raw).toString
    taskPartitionFiles
      .computeIfAbsent(qualDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)
    stagingPath
  }

  /**
   * FIX #3: writes a _pending_<jobId>_<taskAttemptId> file for THIS TASK ONLY.
   * Multiple tasks writing to the same partition each write their own file.
   * No race condition, no overwriting other tasks' pending lists.
   */
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)
    val superMsg     = super.commitTask(taskContext)  // staging → final rename
    val taskAttemptId = taskContext.getTaskAttemptID.toString

    val snapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (partDir, files) =>
      snapshot.put(partDir, new java.util.ArrayList[String](files))
    }

    // Write per-task _pending_ file for each partition written by this task.
    // Recovery PATH A unions all _pending_<jobId>_* to get complete file list.
    snapshot.forEach { (partDir, files) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(taskContext.getConfiguration)
      writePendingFile(partFs, partPath, taskAttemptId, files.asScala.toSeq)
      logDebug(
        s"ManifestCommitProtocol: wrote _pending_ (${files.size()} files) " +
          s"at $partDir task=$taskAttemptId")
    }

    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    new TaskCommitMessage(CombinedCommitPayloadV5(superMsg.obj, snapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
    // Leave _started_ and _pending_ — recovery PATH A will clean them up
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  UTILITIES
  // ═══════════════════════════════════════════════════════════════════════════

  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning(s"ManifestCommitProtocol: executor state null — setupTask() as recovery " +
        s"task=${taskContext.getTaskAttemptID}")
      setupTask(taskContext)
    }
  }

  /** Iterative: clean _started_ files with no matching _committed_ and no _pending_ files. */
  private def cleanupOrphanedStartedIterative(dir: Path, fs: FileSystem): Unit = {
    val queue = new java.util.ArrayDeque[Path]()
    queue.add(dir)

    while (!queue.isEmpty) {
      val current  = queue.poll()
      val statuses = try { fs.listStatus(current) } catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      val committedTids: Set[String] = files
        .filter(_.getPath.getName.startsWith("_committed_"))
        .map(_.getPath.getName.stripPrefix("_committed_")).toSet

      val pendingTids: Set[String] = files
        .filter(_.getPath.getName.startsWith("_pending_"))
        .map { s =>
          val parts = s.getPath.getName.stripPrefix("_pending_").split("_")
          if (parts.length >= 1) parts(0) else ""
        }.filter(_.nonEmpty).toSet

      files
        .filter(_.getPath.getName.startsWith("_started_"))
        .foreach { s =>
          val tid = s.getPath.getName.stripPrefix("_started_")
          // Orphaned = no _committed_, no _pending_ (recovery already handled it or
          // it pre-dates our _pending_ scheme — safe to remove)
          if (!committedTids.contains(tid) && !pendingTids.contains(tid)) {
            safeDelete(fs, s.getPath)
            logInfo(s"ManifestCommitProtocol: removed orphaned ${s.getPath}")
          }
        }

      subDirs
        .filterNot(s => { val n = s.getPath.getName; n.startsWith("_") || n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }
  }

  private def cleanupOldManifestsInDir(dir: Path, fs: FileSystem): Unit =
    try {
      fs.listStatus(dir)
        .filter { s =>
          val n = s.getPath.getName
          (n.startsWith("_committed_") || n.startsWith("_started_") ||
            n.startsWith("_pending_")) && !n.endsWith(jobId) &&
            !n.contains(s"_${jobId}_")
        }
        .foreach(s => safeDelete(fs, s.getPath))
    } catch { case _: Exception => }

  private def safeDelete(fs: FileSystem, path: Path): Unit =
    try { fs.delete(path, false) }
    catch { case e: Exception =>
      logWarning(s"ManifestCommitProtocol: cannot delete $path: ${e.getMessage}") }

  private def writeJson(fs: FileSystem, path: Path, json: String): Unit = {
    val out = fs.create(path, true)
    try { out.write(json.getBytes("UTF-8")) } finally { out.close() }
  }

  private def readFileContent(path: Path, fs: FileSystem, maxBytes: Int): String = {
    val in = fs.open(path)
    try {
      val available = math.min(in.available().max(0), maxBytes)
      val buf = new Array[Byte](available)
      in.readFully(buf)
      new String(buf, "UTF-8").trim
    } finally { in.close() }
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
        else inner.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\""))
          .filter(_.nonEmpty).toSeq
    }
  }
}


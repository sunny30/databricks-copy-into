package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest


import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import java.time.Instant
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.JavaConverters._
import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
//  Top-level types — outside the class so pattern match survives serialization
// ─────────────────────────────────────────────────────────────────────────────

final case class CombinedCommitPayloadV6(
                                        superObj:       Any,
                                        partitionFiles: java.util.HashMap[String, java.util.List[String]]
                                      )

final case class StartedManifestV6(
                                  jobId:         String,
                                  partitionPath: String,
                                  startedAt:     String,
                                  outputPath:    String,
                                  dynamic:       Boolean,
                                  pendingFiles:  Seq[String]
                                )

final case class PendingTaskManifestV6(
                                      jobId:         String,
                                      taskAttemptId: String,
                                      partitionPath: String,
                                      startedAt:     String,
                                      pendingFiles:  Seq[String]
                                    )

final case class CommittedManifestV6(
                                    tid:          String,
                                    addedFiles:   Seq[String],
                                    removedFiles: Seq[String]
                                  )

// ─────────────────────────────────────────────────────────────────────────────
//  ManifestCommitProtocol
// ─────────────────────────────────────────────────────────────────────────────

class ManifestFileCommitProtocolV6(
                              jobId:                    String,
                              outputPath:               String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ── DRIVER-SIDE @transient ─────────────────────────────────────────────────
  @transient private var outputDir:         Path            = _
  private var jobFs:             FileSystem      = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)
  @transient private var sharedPool:        ExecutorService = _
  @transient private var deleteParallelism: Int             = 16
  @transient private var writeParallelism:  Int             = 16

  // ── FIX #2: Use RELATIVE paths as keys — eliminates scheme/hostname mismatch ──
  // Both collectOldDirsBFS and newTaskTempFile will store paths relative to
  // outputPath (e.g. "year=2024/month=01/day=15" not "s3a://bucket/t/year=...").
  // Relative paths are scheme-independent and always match.
  @transient private val pendingDeleteRelDirs =
  java.util.Collections.newSetFromMap(
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean]())

  // ── EXECUTOR-SIDE — initialized in setupTask() only ───────────────────────
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
    val out = fs.create(path, true)
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
          .filter(_.nonEmpty).toSeq
    }
  }

  // ── FIX #2 helpers: relative path computation ─────────────────────────────

  /**
   * Converts an absolute qualified path to a path relative to outputPath.
   * e.g. "s3a://bucket/table/year=2024/month=01" -> "year=2024/month=01"
   *      "s3a://bucket/table" -> "" (root)
   * Uses jobFs (driver-side only).
   */
  private def toRelKey(absPath: Path): String = {
//    if(jobFs == null)
//      jobFs = absPath.getFileSystem()getFileSystem
    val qualBase = jobFs.makeQualified(new Path(outputPath)).toString.stripSuffix("/")
    val qualPath = jobFs.makeQualified(absPath).toString.stripSuffix("/")
    qualPath.stripPrefix(qualBase).stripPrefix("/")
  }

  /**
   * Converts a relative key back to an absolute Path for FS operations.
   */
  private def toAbsPath(relKey: String): Path =
    if (relKey.isEmpty) new Path(outputPath)
    else new Path(outputPath, relKey)

  // ── Same helper for executor side using task configuration ─────────────────
  private def toRelKeyExec(absPath: Path, taskContext: TaskAttemptContext): String = {
    val partFs   = absPath.getFileSystem(taskContext.getConfiguration)
    val qualBase = partFs.makeQualified(new Path(outputPath)).toString.stripSuffix("/")
    val qualPath = partFs.makeQualified(absPath).toString.stripSuffix("/")
    qualPath.stripPrefix(qualBase).stripPrefix("/")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  PARALLEL EXECUTION — single shared pool per job
  // ═══════════════════════════════════════════════════════════════════════════

  private def submitTask[T](body: => T): Future[T] =
    sharedPool.submit(new Callable[T] { def call(): T = body })

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
  //  _started_ WRITE / PARSE
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
   * Updates partition _started_ with this task's file list BEFORE super.commitTask().
   * Reads existing, unions with new files, rewrites.
   * Makes _started_ a reliable recovery source independent of _pending_.
   * Best-effort — race condition between concurrent tasks is acceptable
   * because _pending_ is the authoritative per-task source.
   */
  private def updatePartitionStarted(
                                      partDir:     String,
                                      relKey:      String,
                                      files:       Seq[String],
                                      taskContext: TaskAttemptContext): Unit = {
    val partPath    = new Path(partDir)
    val partFs      = partPath.getFileSystem(taskContext.getConfiguration)
    val startedPath = new Path(partPath, s"_started_$jobId")

    val existing: Seq[String] = try {
      if (partFs.exists(startedPath)) parseStartedFile(startedPath, partFs).pendingFiles
      else Seq.empty
    } catch { case _: Exception => Seq.empty }

    writeStartedFile(partFs, startedPath, relKey, (existing ++ files).distinct)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  _pending_ WRITE / PARSE
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
  //  _committed_ WRITE / PARSE
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
                                      rootFs:            FileSystem,
                                      rootDir:           Path,
                                      // keyed by RELATIVE path
                                      partitionFiles:    java.util.HashMap[String, java.util.List[String]],
                                      allRemoved:        Seq[String]): Unit = {

    val addedRelative = partitionFiles.asScala.flatMap { case (relKey, files) =>
      files.asScala.map(f => if (relKey.isEmpty) f else s"$relKey/$f")
    }.toSeq

    val partitionsJson = partitionFiles.asScala.map { case (relKey, files) =>
      val fStr = files.asScala.map(f => s""""$f"""").mkString(",")
      s""""$relKey":[$fStr]"""
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
  //  ITERATIVE BFS — collects old dir RELATIVE keys
  //  FIX #2: stores relative paths so they match mergedPartitionFiles keys
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
        // FIX #2: store as relative key — no scheme/hostname in key
        val relKey = toRelKey(dir)
        pendingDeleteRelDirs.add(relKey)
        logDebug(s"ManifestCommitProtocol.collectOld: relKey='$relKey' absPath=$dir")
      }

      subDirs
        .withFilter(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
        .foreach(s => queue.add(s.getPath))
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  ITERATIVE ANCESTOR CLEANUP
  // ═══════════════════════════════════════════════════════════════════════════

  private def cleanupEmptyAncestorsBFS(
                                        fs:      FileSystem,
                                        startAt: Path,
                                        stopAt:  Path): Unit = {

    val qualStop = try { fs.makeQualified(stopAt) }
    catch { case _: Exception => return }
    var current  = try { fs.makeQualified(startAt) }
    catch { case _: Exception => return }

    while (current != qualStop && current.toString.startsWith(qualStop.toString)) {
      try {
        if (!fs.exists(current)) { current = current.getParent }
        else {
          val hasReal = fs.listStatus(current).exists { s =>
            val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".")
          }
          if (hasReal) return
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
  //  RECOVERY — iterative BFS, parallel per-dir
  // ═══════════════════════════════════════════════════════════════════════════

  private def recoverBFS(rootDir: Path, fs: FileSystem): Unit = {
    val rootPath = new Path(outputPath)
    val bfsQueue = new LinkedBlockingQueue[Path]()
    val futures  = new java.util.concurrent.ConcurrentLinkedQueue[Future[_]]()

    bfsQueue.add(rootDir)

    while (!bfsQueue.isEmpty) {
      val dir      = bfsQueue.poll()
      val statuses = try { fs.listStatus(dir) }
      catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      val committedByTid: Map[String, Path] = files
        .withFilter(_.getPath.getName.startsWith("_committed_"))
        .map(s => s.getPath.getName.stripPrefix("_committed_") -> s.getPath).toMap

      val startedByTid: Map[String, Path] = files
        .withFilter(_.getPath.getName.startsWith("_started_"))
        .map(s => s.getPath.getName.stripPrefix("_started_") -> s.getPath).toMap

      val pendingByTid: Map[String, Seq[Path]] = files
        .filter { s =>
          val n = s.getPath.getName
          n.startsWith("_pending_") && n.stripPrefix("_pending_").split("_").length >= 2
        }
        .groupBy(s => s.getPath.getName.stripPrefix("_pending_").split("_").head)
        .mapValues(_.map(_.getPath).toSeq)

      val filesList        = files.toSeq
      val allCommittedCapt = committedByTid

      startedByTid.foreach { case (tid, startedPath) =>
        val dirC         = dir
        val pendingPaths = pendingByTid.getOrElse(tid, Seq.empty)
        val filesCapt    = filesList

        futures.add(submitTask {
          committedByTid.get(tid) match {
            case None =>
              recoverPathA(dirC, fs, tid, startedPath, pendingPaths, filesCapt, rootPath)
            case Some(committedPath) =>
              recoverPathB(dirC, fs, tid, startedPath, committedPath, allCommittedCapt, rootPath)
          }
        })
      }

      subDirs
        .withFilter(s => { val n = s.getPath.getName; !n.startsWith("_") && !n.startsWith(".") })
        .foreach(s => bfsQueue.add(s.getPath))
    }

    futures.asScala.foreach { f =>
      try { f.get(30, TimeUnit.MINUTES) }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover: ${e.getMessage}") }
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

    val fromPending: Set[String] = pendingPaths.flatMap { pp =>
      try { parsePendingFile(pp, fs).pendingFiles }
      catch { case _: Exception => Seq.empty[String] }
    }.toSet

    val fromStarted: Set[String] =
      try { parseStartedFile(startedPath, fs).pendingFiles.toSet }
      catch { case _: Exception => Set.empty[String] }

    val tidPattern = s"-tid-$tid-"
    val fromScan: Set[String] = filesInDir
      .withFilter(s => isDataFile(s.getPath.getName) && s.getPath.getName.contains(tidPattern))
      .map(_.getPath.getName).toSet

    val toDelete = fromPending ++ fromStarted ++ fromScan
    val deleted  = new AtomicInteger(0)
    val tasks: Seq[() => Unit] = toDelete.toSeq.map { name => () =>
      val p = new Path(dir, name)
      try {
        if (fs.exists(p)) {
          fs.delete(p, false)
          deleted.incrementAndGet()
          logInfo("")
      }
      }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[A]: cannot delete $p: ${e.getMessage}") }
    }
    parallelExec(tasks, s"recoverA[$dir]")
    pendingPaths.foreach(pp => safeDelete(fs, pp))
    safeDelete(fs, startedPath)
    logInfo(
      s"ManifestCommitProtocol.recover[A]: $dir tid=$tid deleted=${deleted.get}/${toDelete.size} " +
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
    val tasks: Seq[() => Unit] = manifest.removedFiles.withFilter(isDataFile).map { name => () =>
      val p = new Path(dir, name)
      try { if (fs.exists(p)) {
        fs.delete(p, false);
        deleted.incrementAndGet()
        logInfo("")
      }
      }
      catch { case e: Exception =>
        logWarning(s"ManifestCommitProtocol.recover[B]: cannot delete $p: ${e.getMessage}") }
    }
    parallelExec(tasks, s"recoverB[$dir]")

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
        logWarning(s"ManifestCommitProtocol.recover[B]: cannot delete $dir: ${e.getMessage}") }
      return
    }

    safeDelete(fs, startedPath)
    logInfo(
      s"ManifestCommitProtocol.recover[B]: $dir tid=$tid " +
        s"deleted=${deleted.get}/${manifest.removedFiles.size}")
  }

  private def cleanupOrphanedStartedBFS(dir: Path, fs: FileSystem): Unit = {
    val queue = new java.util.ArrayDeque[Path]()
    queue.add(dir)
    while (!queue.isEmpty) {
      val current  = queue.poll()
      val statuses = try { fs.listStatus(current) }
      catch { case _: Exception => Array.empty[FileStatus] }
      val (subDirs, files) = statuses.partition(_.isDirectory)

      val committedTids = files
        .withFilter(_.getPath.getName.startsWith("_committed_"))
        .map(_.getPath.getName.stripPrefix("_committed_")).toSet
      val pendingTids = files
        .withFilter(_.getPath.getName.startsWith("_pending_"))
        .flatMap(s => s.getPath.getName.stripPrefix("_pending_").split("_").headOption)
        .toSet

      files.withFilter(_.getPath.getName.startsWith("_started_"))
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
    sharedPool = Executors.newFixedThreadPool(poolSize, new ThreadFactory {
      val counter = new AtomicInteger(0)
      def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"manifest-commit-${counter.incrementAndGet()}")
        t.setDaemon(true); t
      }
    })

    // Recovery BEFORE cleanupOrphanedStarted (PATH A needs _started_ to be intact)
    recoverBFS(outputDir, jobFs)
    cleanupOrphanedStartedBFS(outputDir, jobFs)

    writeStartedFile(jobFs, new Path(outputDir, s"_started_$jobId"), outputPath)
//    logInfo(
//      s"ManifestCommitProtocol.setupJob: path=$outputPath jobId=$jobId " +
//        s"dynamic=$dynamicPartitionOverwrite poolSize=$poolSize")
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    collectOldDirsBFS(fs, path)
    logInfo(
      s"ManifestCommitProtocol.deleteWithJob: deferred $path → " +
        s"${pendingDeleteRelDirs.size()} relative dir keys recorded")
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    val rootPath = new Path(outputPath)

    // ── Step 1: unwrap CombinedCommitPayloadV6 ─────────────────────────────────
    // mergedPartitionFiles keyed by RELATIVE path (from commitTask)
    val mergedPartitionFiles =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayloadV6 =>
          cp.partitionFiles.forEach { (relKey, files) =>
            mergedPartitionFiles.compute(relKey, (_, existing) => {
              val list = if (existing == null) new java.util.ArrayList[String]() else existing
              list.addAll(files); list
            })
          }
          new TaskCommitMessage(cp.superObj)
        case _ => msg
      }
    }

    // ── Step 2: capture REMOVED lists ────────────────────────────────────────
    //
    // FIX #1 — FileOutputCommitter v2 (default in Hadoop 3.x):
    //   With algorithm.version=2, commitTask() renames files directly to the
    //   FINAL output dir (NOT to staging). By the time we reach commitJob(),
    //   ALL new files are already at their final location alongside old files.
    //   If we list now without filtering, removedByPartition captures new files
    //   too, and step 6 deletes new data.
    //
    //   Fix: after listing, subtract the new file set from the result.
    //   This is order-independent: works whether listing is before or after
    //   super.commitJob() and regardless of algorithm version.
    //
    // For DYNAMIC: list from mergedPartitionFiles dirs (dynamic uses abs-path
    //   staging, so new files are NOT at final location before super runs). ✓
    // For STATIC:  list from pendingDeleteRelDirs. Written partition dirs may
    //   already contain new files (v2). Stale partition dirs never contain new
    //   files (nothing was written there). Filter handles both correctly.
    val removedByPartition =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.List[String]]()

    val listingRelKeys: Seq[String] =
      if (dynamicPartitionOverwrite) mergedPartitionFiles.keySet().asScala.toSeq
      else pendingDeleteRelDirs.asScala.toSeq

    val listTasks: Seq[() => Unit] = listingRelKeys.map { relKey => () =>
      val absPath = toAbsPath(relKey)
      val partFs  = absPath.getFileSystem(jobContext.getConfiguration)
      val allFiles = listDataFileNames(partFs, absPath)

      // FIX #1: subtract new files written by THIS job from the listing.
      // New files: those whose names are in mergedPartitionFiles[relKey].
      // Old files: everything else.
      val newFileSet = mergedPartitionFiles
        .getOrDefault(relKey, java.util.Collections.emptyList[String]())
        .asScala.toSet
      val oldFiles = allFiles.filterNot(f => newFileSet.contains(f))

      if (oldFiles.nonEmpty)
        removedByPartition.put(relKey, oldFiles.toList.asJava)

      logInfo("")
    }
    parallelExec(listTasks, "step2-listOld")

    // ── Step 3: super.commitJob() ─────────────────────────────────────────────
    // For v1: renames staged files to final (old + new coexist until step 6).
    // For v2: no-op at job level (files already moved in commitTask).
    // For dynamic: deletes old partition dirs + renames staging → final.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: PARALLEL write per-partition _committed_ (AFTER super) ───────
    // Written after super so that in all cases (v1/v2/dynamic) the new data
    // files are at their final location when _committed_ is created.
    val writeCommittedTasks: Seq[() => Unit] =
    mergedPartitionFiles.asScala.toSeq.map { case (relKey, addedFiles) => () =>
      val absPath = toAbsPath(relKey)
      val partFs  = absPath.getFileSystem(jobContext.getConfiguration)
      val removed = removedByPartition.getOrDefault(
        relKey, java.util.Collections.emptyList[String]())
      writePartitionCommittedFile(
        partFs, absPath,
        addedFiles.asScala.toSeq,
        removed.asScala.toSeq)
    }
    parallelExec(writeCommittedTasks, "step4-writeCommitted")

    // ── Step 5: write root-level _committed_ ──────────────────────────────────
    val allRemoved: Seq[String] =
      removedByPartition.values().asScala.flatMap(_.asScala).toSeq
    writeRootCommittedFile(jobFs, outputDir,
      new java.util.HashMap(mergedPartitionFiles), allRemoved)

    // ── Step 6: PARALLEL delete old data (STATIC overwrite only) ─────────────
    if (!dynamicPartitionOverwrite) {

      val fileDeleteTargets =
        new java.util.concurrent.ConcurrentLinkedQueue[(FileSystem, Path)]()
      val staleDirs = mutable.ArrayBuffer.empty[(FileSystem, Path)]

      pendingDeleteRelDirs.asScala.foreach { relKey =>
        val absPath = toAbsPath(relKey)
        val dirFs   = absPath.getFileSystem(jobContext.getConfiguration)

        if (mergedPartitionFiles.containsKey(relKey)) {
          // ── Case A: written partition ─────────────────────────────────────
          // Delete only the specific OLD file names captured in step 2.
          // Dir contains new data + _committed_ — never delete the whole dir.
          removedByPartition
            .getOrDefault(relKey, java.util.Collections.emptyList[String]())
            .forEach { name => fileDeleteTargets.add((dirFs, new Path(absPath, name))) }

        } else {
          // ── Case B: stale partition ───────────────────────────────────────
          // FIX #3: before recursive delete, check if this stale dir is an
          // ANCESTOR of any written partition. If so, do NOT recursively
          // delete — that would wipe new data in the written child dirs.
          //
          // Example: pendingDeleteRelDirs has "p=1" (has data files directly)
          //   AND mergedPartitionFiles has "p=1/q=a" (written child).
          // Without this check, deleting "p=1" recursively kills "p=1/q=a".
          //
          // In practice this only occurs for tables with data at intermediate
          // partition levels (rare but must be handled correctly).
          val keyPrefix = if (relKey.isEmpty) "" else relKey + "/"
          val hasWrittenDescendant = mergedPartitionFiles.keySet().asScala
            .exists(k => k.startsWith(keyPrefix) || k == relKey)

          if (hasWrittenDescendant) {
            // Do NOT recursively delete — only delete old files at this level
            logWarning(
              s"ManifestCommitProtocol: stale dir $relKey has written descendants; " +
                s"deleting old files by name only (not recursively)")
            removedByPartition
              .getOrDefault(relKey, java.util.Collections.emptyList[String]())
              .forEach { name => fileDeleteTargets.add((dirFs, new Path(absPath, name))) }
          } else {
            staleDirs += ((dirFs, absPath))
          }
        }
      }

      // Parallel delete of old individual files in written partitions
      val fileCount = new AtomicLong(0)
      val fileTasks: Seq[() => Unit] = fileDeleteTargets.asScala.toSeq.map {
        case (fs, path) => () =>
          try {
            if (fs.exists(path)) {
              fs.delete(path, false);
              fileCount.incrementAndGet()
              logInfo("")
            }
          } catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: cannot delete $path: ${e.getMessage}") }
      }
      parallelExec(fileTasks, "step6-fileDelete")
      logInfo(
        s"ManifestCommitProtocol step6: deleted ${fileCount.get}/${fileDeleteTargets.size()} " +
          s"old files (parallelism=$deleteParallelism)")

      // Parallel delete of stale partition dirs
      val dirCount = new AtomicLong(0)
      val dirTasks: Seq[() => Unit] = staleDirs.toSeq.map {
        case (fs, path) => () =>
          try {
            if (fs.exists(path)) {
              fs.delete(path, true);
              dirCount.incrementAndGet()
              logInfo("")
            }
          } catch { case e: Exception =>
            logWarning(s"ManifestCommitProtocol: cannot delete stale $path: ${e.getMessage}") }
      }
      parallelExec(dirTasks, "step6-staleDelete")
      logInfo(
        s"ManifestCommitProtocol step6: deleted ${dirCount.get}/${staleDirs.size} stale dirs")

      // Clean empty ancestor dirs (serial — few ancestors, cheap listStatus)
      staleDirs.foreach { case (fs, dirPath) =>
        cleanupEmptyAncestorsBFS(fs, dirPath.getParent, rootPath)
      }

      pendingDeleteRelDirs.clear()
    }

    // ── Step 7: PARALLEL delete _started_ + _pending_ ────────────────────────
    val cleanupTargets =
      new java.util.concurrent.ConcurrentLinkedQueue[(FileSystem, Path)]()

    cleanupTargets.add((jobFs, new Path(outputDir, s"_started_$jobId")))

    if (dynamicPartitionOverwrite) {
      val cleanOldTasks: Seq[() => Unit] =
        mergedPartitionFiles.keySet().asScala.toSeq.map { relKey => () =>
          val p   = toAbsPath(relKey)
          val pFs = p.getFileSystem(jobContext.getConfiguration)
          cleanupOldManifestsInDir(p, pFs)
        }
      parallelExec(cleanOldTasks, "step7-cleanOldManifests")
    } else {
      val collectTasks: Seq[() => Unit] =
        mergedPartitionFiles.keySet().asScala.toSeq.map { relKey => () =>
          val p   = toAbsPath(relKey)
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
    pendingDeleteRelDirs.clear()
    try {
      if (sharedPool != null && !sharedPool.isShutdown) sharedPool.shutdownNow()
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

  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir:         Option[String],
                                spec:        FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFile(taskContext, dir, spec)

    // FIX #2: key = RELATIVE path, matching pendingDeleteRelDirs keys.
    // dir is already relative to outputPath (e.g. "year=2024/month=01/day=15").
    val relKey = dir.getOrElse("")

    if (seenPartitionDirs.putIfAbsent(relKey, java.lang.Boolean.TRUE) == null) {
      val absPath = new Path(outputPath, relKey)
      val partFs  = absPath.getFileSystem(taskContext.getConfiguration)
      writeStartedFile(partFs, new Path(absPath, s"_started_$jobId"),
        partitionPath = relKey, pendingFiles = Seq.empty)
      logDebug(
        s"ManifestCommitProtocol: wrote _started_(empty) relKey='$relKey' " +
          s"task=${taskContext.getTaskAttemptID}")
    }

    taskPartitionFiles
      .computeIfAbsent(relKey, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)

    stagingPath
  }

  override def newTaskTempFileAbsPath(
                                       taskContext:  TaskAttemptContext,
                                       absoluteDir:  String,
                                       spec:         FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    // For abs-path writes, use the relative form of absoluteDir as key
    val relKey = toRelKeyExec(new Path(absoluteDir), taskContext)
    taskPartitionFiles
      .computeIfAbsent(relKey, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)
    stagingPath
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    val taskAttemptId = taskContext.getTaskAttemptID.toString

    // Snapshot — keys are RELATIVE paths
    val snapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (relKey, files) =>
      snapshot.put(relKey, new java.util.ArrayList[String](files))
    }

    // Phase 1: update partition _started_ with file list BEFORE staging→final rename.
    // If cluster crashes after rename but before _pending_ is written, recovery
    // PATH A reads _started_ and finds these filenames to clean up.
    snapshot.forEach { (relKey, files) =>
      val absPath = new Path(outputPath, relKey)
      updatePartitionStarted(absPath.toString, relKey, files.asScala.toSeq, taskContext)
    }

    // Phase 2: staging → final rename
    val superMsg = super.commitTask(taskContext)
    // Files are now at final location on disk.

    // Phase 3: write _pending_<jobId>_<taskAttemptId> per partition (authoritative).
    // One file per task — never overwritten by other tasks.
    snapshot.forEach { (relKey, files) =>
      val absPath = new Path(outputPath, relKey)
      val partFs  = absPath.getFileSystem(taskContext.getConfiguration)
      writePendingFile(partFs, absPath, taskAttemptId, files.asScala.toSeq)
      logDebug(
        s"ManifestCommitProtocol: wrote _pending_ ${files.size()} files " +
          s"relKey='$relKey' task=$taskAttemptId")
    }

    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    new TaskCommitMessage(CombinedCommitPayloadV6(superMsg.obj, snapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
  }

  // ── Utilities ──────────────────────────────────────────────────────────────

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
//```
//
//---
//
//## Three bugs — exact traces
//  ```
//BUG 1: FileOutputCommitter v2 "same file" (default in Hadoop 3.x)
//─────────────────────────────────────────────────────────────────
//commitTask() with v2:
//  super.commitTask() renames files to FINAL location immediately
//e.g. p=1/q=a/part-NEW-tid-JOBID.parquet is at final loc
//
//commitJob() step 2 (BEFORE fix):
//  listDataFileNames(p=1/q=a) → [part-OLD.parquet, part-NEW-tid-JOBID.parquet]
//  removedByPartition["p=1/q=a"] = [OLD, NEW]  ← NEW wrongly included
//
//step 6:
//  fileDeleteTargets includes part-NEW-tid-JOBID.parquet
//fs.delete(part-NEW-tid-JOBID.parquet) → NEW DATA DELETED!
//
//FIX: val newFileSet = mergedPartitionFiles.getOrDefault(relKey, empty).toSet
//val oldFiles = allFiles.filterNot(newFileSet.contains)
//
//
//BUG 2: Path normalization — "files not getting deleted"
//───────────────────────────────────────────────────────
//pendingDeleteDirs key:  "s3a://bucket/table/year=2024/month=01"  (from fs.listStatus)
//mergedPartitionFiles key: "s3://bucket/table/year=2024/month=01" (from outputPath + dir)
//^^ different scheme!
//
//containsKey("s3a://...") on map with key "s3://" → FALSE
//ALL written partitions treated as Case B (stale) → recursive delete
//  ALL new data deleted!
//
//  FIX: Use RELATIVE keys ("year=2024/month=01") everywhere.
//dir parameter in newTaskTempFile is already relative.
//  toRelKey() strips the outputPath prefix from absolute paths.
//
//
//BUG 3: Stale ancestor wipes written descendant
//────────────────────────────────────────────────
//Table has data at multiple levels:
//  p=1/part-old.parquet           ← intermediate level data
//p=1/q=a/part-old.parquet       ← leaf level data
//
//pendingDeleteRelDirs = {"p=1", "p=1/q=a"}
//mergedPartitionFiles = {"p=1/q=a"}
//
//Step 6 (BEFORE fix):
//  "p=1"     NOT in mergedPartitionFiles → Case B: fs.delete(p=1/, recursive=true)
//  This deletes p=1/q=a/ and ALL NEW DATA inside it!
//    "p=1/q=a" IS in mergedPartitionFiles → Case A: try delete old files from p=1/q=a/
//    p=1/q=a/ was already deleted! "trying to delete same file" = fs.exists(p) = false
//
//  FIX: Before Case B recursive delete, check:
//  val hasWrittenDescendant = mergedPartitionFiles.keySet()
//    .exists(k => k.startsWith(relKey + "/"))
//  if (hasWrittenDescendant) → Case A behavior (delete only old files at this level)
//  else → Case B behavior (recursive delete)
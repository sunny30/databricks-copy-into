package org.apache.spark.sql.hive.plan.spark.sql.ptotocol

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import scala.collection.mutable
import scala.collection.JavaConverters._

case class CombinedCommitPayload(
                                  superObj: Any, // super's Tuple2
                                  partitionFiles: java.util.HashMap[String, java.util.List[String]] // our manifest data
                                )


/**
 * ManifestCommitProtocol — corrected for Spark 3.5.
 *
 * Key fixes over previous versions:
 *
 * 1. Override newTaskTempFile(FileNameSpec) — the Spark 3.5 signature.
 *    The old (ext: String) variant is deprecated and NOT called by Spark 3.5
 *    for partitioned writes. If you override the wrong signature your body
 *    never executes, _started_ files are never written, and taskPartitionFiles
 *    stays empty.
 *
 * 2. commitTask() correctly extracts partition paths from super's Tuple2.
 *    super.commitTask() returns:
 *      TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
 *    For partitioned writes (partitionBy), files go through newTaskTempFile
 *    with a dir → they land in partitionPaths (_2), NOT addedAbsPathFiles (_1).
 *    We must read _2 to know which partitions this task wrote to, then
 *    derive real file paths by listing the staging directory.
 *
 * 3. @transient executor-side fields initialized in setupTask(), not inline.
 */
class ManifestCommitProtocol(
                              jobId: String,
                              outputPath: String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ── Serialized fields (driver + executor) ─────────────────────────────────
  // None needed beyond what super already serializes (jobId, outputPath, etc.)

  // ── Driver-side @transient — initialized in setupJob() ────────────────────
  @transient private var outputDir: Path   = _
  @transient private var jobFs: FileSystem = _
  @transient private val pendingDeletes    = mutable.Buffer.empty[Path]

  // ── Executor-side @transient — initialized in setupTask() ─────────────────
  // MUST NOT be initialized inline — they are null after deserialization.

  // partitionDir (absolute) → list of filenames written in this task
  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String,
    java.util.concurrent.CopyOnWriteArrayList[String]] = _

  // Partition dirs seen in this task — to write _started_ only once per dir
  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ── CombinedCommitPayload carries our data alongside super's Tuple2 ───────
  // Defined at class level (not inside a method) so pattern matching works
  // after serialization/deserialization.


  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)
    cleanupOrphanedStarted(outputDir, jobFs)
    touchFile(jobFs, new Path(outputDir, s"_started_$jobId"))
    logInfo(s"ManifestCommitProtocol.setupJob jobId=$jobId path=$outputPath")
  }

  // Defer partition/directory deletions to commitJob — preserves old data on failure
  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    pendingDeletes += path
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    // ── Split CombinedCommitPayload → (superMessages, our partition data) ──
    val mergedPartitionFiles =
      new java.util.HashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case combined: CombinedCommitPayload =>
          combined.partitionFiles.forEach { (partDir, files) =>
            mergedPartitionFiles
              .computeIfAbsent(partDir, _ => new java.util.ArrayList[String]())
              .addAll(files)
          }
          new TaskCommitMessage(combined.superObj) // restore Tuple2 for super

        case _ =>
          msg // empty task or unexpected — pass through unchanged
      }
    }

    // Step 1: write per-partition _committed_ (before super moves files)
    mergedPartitionFiles.forEach { (partDir, files) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      writePartitionCommitted(partFs, partPath, files.asScala.toSeq)
    }

    // Step 2: write root-level _committed_
    writeRootCommitted(jobFs, outputDir, mergedPartitionFiles)

    // Step 3: super.commitJob() moves staged files to final locations
    // superMessages carry the original Tuple2 — no ClassCastException
    super.commitJob(jobContext, superMessages)

    // Step 4: deferred deletes (old data removal — after new data is committed)
    if (dynamicPartitionOverwrite) {
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p  = new Path(partDir)
        val fs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifests(p, fs)
      }
    } else {
      pendingDeletes.foreach { path =>
        try { if (jobFs.exists(path)) jobFs.delete(path, true) }
        catch { case e: Exception =>
          logWarning(s"ManifestCommitProtocol: delete failed $path: ${e.getMessage}") }
      }
      pendingDeletes.clear()
    }

    // Step 5: delete _started_ (signals clean completion to readers)
    jobFs.delete(new Path(outputDir, s"_started_$jobId"), false)
    mergedPartitionFiles.keySet().forEach { partDir =>
      val p  = new Path(partDir)
      val fs = p.getFileSystem(jobContext.getConfiguration)
      fs.delete(new Path(p, s"_started_$jobId"), false)
    }

    logInfo(s"ManifestCommitProtocol.commitJob complete path=$outputPath")
  }

  override def abortJob(jobContext: JobContext): Unit = {
    pendingDeletes.clear() // do NOT delete old data on failure
    super.abortJob(jobContext)
    // _started_ left in place — signals failed write to readers
    logInfo(s"ManifestCommitProtocol.abortJob: old data preserved at $outputPath")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext) // initializes super's addedAbsPathFiles + partitionPaths

    // Initialize executor-side state HERE — after deserialization @transient = null
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  // ── THE KEY FIX: override the FileNameSpec variant (Spark 3.5) ────────────
  // The deprecated (ext: String) variant is never called by Spark 3.5 for
  // partitioned writes. If you only override that one, this body never fires.
  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir: Option[String],
                                spec: FileNameSpec): String = {

    ensureTaskState(taskContext)

    // Let super handle the actual path construction and staging directory routing
    val path    = super.newTaskTempFile(taskContext, dir, spec)

    // The partition directory is the absolute path of dir relative to outputPath
    // For unpartitioned: dir = None → partDir = outputPath
    // For partitioned:   dir = Some("year=2024/month=01") → absolute partition path
    val partDir = dir match {
      case Some(d) =>
        // Reconstruct the absolute partition path.
        // super.newTaskTempFile puts the file under stagingDir/d/filename.
        // The FINAL destination (what goes in the manifest) is outputPath/d.
        new Path(outputPath, d).toString
      case None =>
        outputPath
    }

    // Write partition-level _started_ once per partition per task
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      val taskFs   = partPath.getFileSystem(taskContext.getConfiguration)
      cleanupOrphanedStarted(partPath, taskFs)
      touchFile(taskFs, new Path(partPath, s"_started_$jobId"))
      logDebug(s"ManifestCommitProtocol: wrote _started_ at $partDir")
    }

    // Track only the filename (not full path) under the final partition dir
    val fileName = new Path(path).getName
    taskPartitionFiles
      .computeIfAbsent(partDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(fileName)

    path // return super's path unchanged
  }

  // Also override the AbsPath variant to track those files too
  override def newTaskTempFileAbsPath(
                                       taskContext: TaskAttemptContext,
                                       absoluteDir: String,
                                       spec: FileNameSpec): String = {

    ensureTaskState(taskContext)
    val path     = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    val fileName = new Path(path).getName

    // For absolute path files the final dir IS the absoluteDir
    taskPartitionFiles
      .computeIfAbsent(absoluteDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(fileName)

    path
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    // super.commitTask() does the Hadoop staging→task rename and returns:
    //   TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
    // where:
    //   addedAbsPathFiles = Map[stagingTmpPath → finalAbsPath]  (for absPath writes)
    //   partitionPaths    = Set[String]  relative partition dirs (for partitionBy writes)
    //                       e.g. Set("year=2024/month=01", "year=2024/month=02")
    val superMessage = super.commitTask(taskContext)

    // Build our partition→files snapshot
    val partitionFilesSnapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (partDir, files) =>
      partitionFilesSnapshot.put(partDir, new java.util.ArrayList(files))
    }

    logDebug(
      s"ManifestCommitProtocol.commitTask: " +
        s"partitions=${partitionFilesSnapshot.size()} " +
        s"files=${partitionFilesSnapshot.values().asScala.map(_.size()).sum} " +
        s"task=${taskContext.getTaskAttemptID}")

    // Clear executor state
    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    // Wrap both payloads: super's Tuple2 + our HashMap
    new TaskCommitMessage(CombinedCommitPayload(superMessage.obj, partitionFilesSnapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
    // Leave partition-level _started_ — signals incomplete task
  }

  // ── Guard ─────────────────────────────────────────────────────────────────

  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning("ManifestCommitProtocol: executor state null — calling setupTask()")
      setupTask(taskContext)
    }
  }

  // ── Manifest writers ──────────────────────────────────────────────────────

  private def writePartitionCommitted(
                                       partFs: FileSystem,
                                       partDir: Path,
                                       fileNames: Seq[String]): Unit = {
    val added = fileNames.map(f => s""""$f"""").mkString(",")
    writeJson(partFs, new Path(partDir, s"_committed_$jobId"),
      s"""{"added":[$added],"removed":[]}""")
  }

  private def writeRootCommitted(
                                  rootFs: FileSystem,
                                  rootDir: Path,
                                  partitionFiles: java.util.HashMap[String, java.util.List[String]]): Unit = {

    val addedRelative = partitionFiles.asScala.flatMap { case (partDir, files) =>
      val rel = partDir.stripPrefix(outputPath).stripPrefix("/")
      files.asScala.map(f => if (rel.isEmpty) f else s"$rel/$f")
    }.toSeq

    val partitionsJson = partitionFiles.asScala.map { case (partDir, files) =>
      val rel  = partDir.stripPrefix(outputPath).stripPrefix("/")
      val fStr = files.asScala.map(f => s""""$f"""").mkString(",")
      s""""$rel":[$fStr]"""
    }.mkString(",")

    val addedStr = addedRelative.map(f => s""""$f"""").mkString(",")
    writeJson(rootFs, new Path(rootDir, s"_committed_$jobId"),
      s"""{"added":[$addedStr],"removed":[],"partitions":{$partitionsJson}}""")
  }

  // ── Utilities ─────────────────────────────────────────────────────────────

  private def touchFile(fs: FileSystem, path: Path): Unit = {
    val out = fs.create(path, true); out.close()
  }

  private def writeJson(fs: FileSystem, path: Path, json: String): Unit = {
    val out = fs.create(path, true)
    try { out.write(json.getBytes("UTF-8")) } finally { out.close() }
  }

  private def cleanupOrphanedStarted(dir: Path, fs: FileSystem): Unit = {
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
  }

  private def cleanupOldManifests(dir: Path, fs: FileSystem): Unit = {
    try {
      fs.listStatus(dir)
        .filter { s =>
          val n = s.getPath.getName
          (n.startsWith("_committed_") || n.startsWith("_started_")) &&
            !n.endsWith(jobId)
        }
        .foreach(s => fs.delete(s.getPath, false))
    } catch { case _: Exception => () }
  }
}
//```
//
//---
//
//## Root cause — two errors in one
//  ```
//Previous code:
//
//  commitTask():
//  super.commitTask()            → returns TaskCommitMessage(Tuple2(...))
//new TaskCommitMessage(HashMap) ← REPLACES the Tuple2 with our HashMap
//
//commitJob():
//  super.commitJob(taskCommits)  ← sees our HashMap, tries to cast to Tuple2
//→ ClassCastException: HashMap cannot be cast to Tuple2
//
//
//  The fix — CombinedCommitPayload:
//
//  commitTask():
//val superMsg = super.commitTask()          → TaskCommitMessage(Tuple2(...))
//val ourData  = snapshot of taskPartitionFiles
//new TaskCommitMessage(
//  CombinedCommitPayload(superMsg.obj, ourData))  ← carries BOTH
//
//commitJob():
//  // Split: reconstruct Tuple2-carrying messages for super
//  superMessages = taskCommits.map { msg =>
//  msg.obj match {
//    case combined: CombinedCommitPayload =>
//      extract ourData → mergedPartitionFiles
//        new TaskCommitMessage(combined.superObj)  ← Tuple2 restored
//  }
//}
//super.commitJob(jobContext, superMessages)  ← sees Tuple2 → no cast error ✅
// then use mergedPartitionFiles for manifest writing
//```
//
//---
//
//## Exact state of the filesystem at each moment
//```
//STATIC OVERWRITE — timeline visible to a concurrent reader:
//
//  T1  setupJob()
//├── write _started_7628..       ← reader sees: write in progress
//└── old data still fully there  ← reader sees old _committed_: reads OLD data ✅
//
//T2  tasks write new files to staging dir
//  └── staging files not visible to readers
//
//T3  commitJob() step 1-2: write new _committed_
//├── _committed_7628.. written   ← reader sees TWO _committed_ files temporarily
//  └── old data still exists       ← reader can still read OLD data ✅
//
//T4  commitJob() step 3: super.commitJob() moves new files to final location
//└── new files now visible
//
//T5  commitJob() step 4: pendingDeletes executed
//  └── old data files deleted      ← reader now reads NEW data ✅
//
//T6  commitJob() step 5-6: delete _started_
//  └── _started_7628.. deleted     ← write fully complete
//
//
//FAILED WRITE — timeline:
//
//  T1  setupJob() → _started_ written, old data intact
//
//T2  tasks write to staging (some fail)
//
//T3  abortJob()
//├── pendingDeletes.clear()      ← NO deletions happen
//├── staging files cleaned by super.abortJob()
//└── _started_ left in place     ← reader: _started_ without _committed_ = failed
//
//Reader behaviour:
//  ManifestAwareFileIndex.listFiles()
//→ sees _started_ without _committed_
//→ if failOnIncompleteWrite=true: throws IllegalStateException
//  → if failOnIncompleteWrite=false: falls back to old _committed_
//reads OLD data ✅
package org.apache.spark.sql.hive.plan.spark.sql.ptotocol

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import scala.collection.mutable
import scala.collection.JavaConverters._

case class CombinedCommitPayload(
                                          superObj: Any, // Tuple2 super expects
                                          partitionFiles: java.util.HashMap[String, java.util.List[String]] // our manifest data
                                        )

class ManifestCommitProtocol(
                              jobId: String,
                              outputPath: String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(
  jobId, outputPath, dynamicPartitionOverwrite) {

  // ── Driver-side ───────────────────────────────────────────────────────────
  @transient private var outputDir: Path   = _
  @transient private var jobFs: FileSystem = _
  @transient private val pendingDeletes    = mutable.Buffer.empty[Path]

  // ── Executor-side — initialized in setupTask() ────────────────────────────
  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String, java.util.concurrent.CopyOnWriteArrayList[String]] = _

  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ═══════════════════════════════════════════════════════════════════════════
  //  The key fix: a wrapper that carries BOTH payloads
  //
  //  super.commitTask() returns TaskCommitMessage(obj = Tuple2(...))
  //  super.commitJob()  casts obj → Tuple2 — must see the Tuple2
  //
  //  Solution: wrap both in a CombinedCommitPayload.
  //  In commitTask()  → build CombinedCommitPayload(superTuple2, ourHashMap)
  //  In commitJob()   → split: give super the Tuple2-wrapped messages,
  //                            use ourHashMap for manifest writing
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Carries both the super's Tuple2 payload and our partition→files map
   * in a single TaskCommitMessage so both can be extracted on the driver.
   */


  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)
    cleanupOrphanedStarted(outputDir, jobFs)
    touchFile(jobFs, new Path(outputDir, s"_started_$jobId"))
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    pendingDeletes += path
    true
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    // ── Split the combined payloads ───────────────────────────────────────
    //
    // Each TaskCommitMessage.obj is either:
    //   CombinedCommitPayload  — written by our commitTask()
    //   something else         — edge case: empty task / task wrote no files
    //
    // We need:
    //   superMessages — TaskCommitMessages with only the Tuple2 inside,
    //                   for super.commitJob() to process correctly
    //   mergedPartitionFiles — our partition→files map for manifest writing

    val mergedPartitionFiles =
      new java.util.HashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {

        case combined: CombinedCommitPayload =>
          // Extract our partition→files data
          combined.partitionFiles.forEach { (partDir, files) =>
            mergedPartitionFiles
              .computeIfAbsent(partDir, _ => new java.util.ArrayList[String]())
              .addAll(files)
          }
          // Reconstruct a TaskCommitMessage with only the Tuple2 for super
          new TaskCommitMessage(combined.superObj)

        case other =>
          // Empty task or unexpected format — pass through unchanged
          msg
      }
    }

    // ── Step 1: Write per-partition _committed_ ───────────────────────────
    mergedPartitionFiles.forEach { (partDir, files) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      writePartitionCommitted(partFs, partPath, files.asScala.toSeq)
    }

    // ── Step 2: Write root-level _committed_ ─────────────────────────────
    writeRootCommitted(jobFs, outputDir, mergedPartitionFiles)

    // ── Step 3: super.commitJob() with Tuple2-carrying messages ──────────
    //
    // This is now safe: superMessages carry the original Tuple2 payload
    // that HadoopMapReduceCommitProtocol.commitJob() expects to cast.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: Deferred deletes ──────────────────────────────────────────
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

    // ── Step 5: Delete _started_ files ───────────────────────────────────
    jobFs.delete(new Path(outputDir, s"_started_$jobId"), false)
    mergedPartitionFiles.keySet().forEach { partDir =>
      val p  = new Path(partDir)
      val fs = p.getFileSystem(jobContext.getConfiguration)
      fs.delete(new Path(p, s"_started_$jobId"), false)
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    pendingDeletes.clear()
    super.abortJob(jobContext)
    // Leave _started_ — signals failed write to readers
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    // Initialize here — NOT inline — because @transient fields
    // are null after Java deserialization to the executor
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir: Option[String],
                                ext: String): String = {

    ensureTaskState(taskContext)
    val path    = super.newTaskTempFile(taskContext, dir, ext)
    val partDir = dir.getOrElse(outputPath)

    // Write partition-level _started_ on first file in this partition
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      val taskFs   = partPath.getFileSystem(taskContext.getConfiguration)
      cleanupOrphanedStarted(partPath, taskFs)
      touchFile(taskFs, new Path(partPath, s"_started_$jobId"))
    }

    // Track filename under its partition dir
    taskPartitionFiles
      .computeIfAbsent(partDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(path).getName)

    path
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    // Call super FIRST — this writes the Hadoop staging → final rename
    // and returns a TaskCommitMessage with a Tuple2 inside
    val superMessage = super.commitTask(taskContext)

    // Build our partition→files snapshot
    val partitionFilesSnapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (partDir, files) =>
      partitionFilesSnapshot.put(partDir, new java.util.ArrayList[String](files))
    }

    // Clear executor state
    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    // Return a CombinedCommitPayload carrying BOTH:
    //   superMessage.obj  — the Tuple2 super.commitJob() needs to cast
    //   partitionFilesSnapshot — our manifest data
    new TaskCommitMessage(CombinedCommitPayload(superMessage.obj, partitionFilesSnapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
  }

  // ── Guard ─────────────────────────────────────────────────────────────────

  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning("ManifestCommitProtocol: executor state null — calling setupTask() as recovery")
      setupTask(taskContext)
    }
  }

  // ── Manifest writers ──────────────────────────────────────────────────────

  private def writePartitionCommitted(
                                       partFs: FileSystem, partDir: Path, fileNames: Seq[String]): Unit = {
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
      val rel   = partDir.stripPrefix(outputPath).stripPrefix("/")
      val fStr  = files.asScala.map(f => s""""$f"""").mkString(",")
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
          }
        }
    } catch { case _: Exception => () }
  }

  private def cleanupOldManifests(dir: Path, fs: FileSystem): Unit = {
    try {
      fs.listStatus(dir)
        .filter { s =>
          val n = s.getPath.getName
          (n.startsWith("_committed_") || n.startsWith("_started_")) && !n.endsWith(jobId)
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
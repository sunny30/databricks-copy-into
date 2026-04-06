package org.apache.spark.sql.hive.plan.spark.sql.connector.manifest

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol.{TaskCommitMessage}
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

import scala.collection.JavaConverters._
import scala.collection.mutable

// ─────────────────────────────────────────────────────────────────────────────
// CombinedCommitPayload — defined at TOP LEVEL (not inside a method or
// commitJob block) so that pattern matching survives Java serialization /
// deserialization on the driver.
// ─────────────────────────────────────────────────────────────────────────────
final case class CombinedCommitPayload(
                                        superObj:       Any,                                                  // super's Tuple2 as-is
                                        partitionFiles: java.util.HashMap[String, java.util.List[String]]     // partDir → [filename]
                                      )

class ManifestFileCommitProtocolV2(
                              jobId: String,
                              outputPath: String,
                              dynamicPartitionOverwrite: Boolean = false
                            ) extends SQLHadoopMapReduceCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {

  // ═══════════════════════════════════════════════════════════════════════════
  //  SERIALIZATION CONTRACT
  //
  //  Committer is Java-serialized on the DRIVER and sent to each EXECUTOR.
  //  @transient fields are NULL after deserialization.
  //
  //  Driver-side @transient  → initialize in setupJob()
  //  Executor-side @transient → initialize in setupTask()   ← CRITICAL
  // ═══════════════════════════════════════════════════════════════════════════

  // ── DRIVER-SIDE — initialized in setupJob() ────────────────────────────────
  @transient private var outputDir: Path   = _
  @transient private var jobFs: FileSystem = _

  // partDir → old data filenames that existed before this write.
  // Populated by deleteWithJob() for STATIC overwrite.
  // Used to: (a) delete ONLY those old files after commit, (b) populate "removed" in manifest.
  // NEVER delete whole dirs — that would also destroy newly committed files.
  @transient private val pendingDeleteByDir =
  new java.util.LinkedHashMap[String, java.util.List[String]]()

  // ── EXECUTOR-SIDE — initialized in setupTask(), NOT inline ────────────────
  // These are null after Java deserialization. setupTask() is the ONLY safe
  // place to initialize them.

  @transient private var taskPartitionFiles
  : java.util.concurrent.ConcurrentHashMap[
    String, java.util.concurrent.CopyOnWriteArrayList[String]] = _

  @transient private var seenPartitionDirs
  : java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean] = _

  // ── Helper ─────────────────────────────────────────────────────────────────
  // A data file is anything not starting with _ or .
  // Excludes: _committed_, _started_, _SUCCESS, _metadata, .crc, etc.
  private def isDataFile(name: String): Boolean =
    !name.startsWith("_") && !name.startsWith(".")

  // List bare data filenames in a directory. Returns empty if dir doesn't exist.
  private def listDataFileNames(fs: FileSystem, dir: Path): Seq[String] =
    try {
      fs.listStatus(dir)
        .filter(s => isDataFile(s.getPath.getName))
        .map(_.getPath.getName)
        .toSeq
    } catch {
      case _: Exception => Seq.empty
    }

  // ═══════════════════════════════════════════════════════════════════════════
  //  JOB LIFECYCLE — DRIVER
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupJob(jobContext: JobContext): Unit = {
    super.setupJob(jobContext)
    outputDir = new Path(outputPath)
    jobFs     = outputDir.getFileSystem(jobContext.getConfiguration)
    cleanupOrphanedStarted(outputDir, jobFs)
    touchFile(jobFs, new Path(outputDir, s"_started_$jobId"))
    logInfo(s"ManifestCommitProtocol.setupJob: path=$outputPath jobId=$jobId")
  }

  // ─────────────────────────────────────────────────────────────────────────
  //  deleteWithJob — called for STATIC overwrite ONLY
  //  (InsertIntoHadoopFsRelationCommand.deleteMatchingPartitions skips this
  //   when dynamicPartitionOverwrite=true)
  //
  //  DEFAULT behaviour: fs.delete(path) immediately — data loss on failure.
  //
  //  OUR behaviour:
  //    1. Capture current filenames BEFORE the write starts → "removed" metric
  //    2. Defer deletion to commitJob AFTER new data is committed
  //    3. In commitJob, delete only those specific OLD files by name,
  //       NOT the whole directory.
  //       (Deleting the whole dir would also destroy newly committed files
  //        and _committed_ that super.commitJob already placed there.)
  // ─────────────────────────────────────────────────────────────────────────
  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    val oldFiles = listDataFileNames(fs, path)
    pendingDeleteByDir.put(path.toString, oldFiles.asJava)
    logInfo(s"ManifestCommitProtocol.deleteWithJob: deferred $path " +
      s"(${oldFiles.size} old data files captured)")
    true  // signal success; actual deletion happens in commitJob
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    // ── Step 1: Unwrap CombinedCommitPayload ──────────────────────────────
    // Split into:
    //   superMessages       — carry original Tuple2 for super.commitJob()
    //   mergedPartitionFiles — our partition→files map for manifest writing
    val mergedPartitionFiles =
    new java.util.HashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayloadV2 =>
          cp.partitionFiles.forEach { (partDir, files) =>
            mergedPartitionFiles
              .computeIfAbsent(partDir, _ => new java.util.ArrayList[String]())
              .addAll(files)
          }
          new TaskCommitMessage(cp.superObj)   // restore Tuple2 for super

        case _ => msg  // empty task or unexpected — pass through unchanged
      }
    }

    // ── Step 2: Capture REMOVED file lists BEFORE super.commitJob() ───────
    //
    // DYNAMIC overwrite:
    //   super.commitJob() will call fs.delete(finalPartDir, true) for each
    //   partition being overwritten. We MUST list old files NOW before they
    //   are deleted, otherwise we lose the "removed" metric forever.
    //
    // STATIC overwrite:
    //   Already captured in deleteWithJob(). No listing needed here.
    val removedByPartition = new java.util.HashMap[String, java.util.List[String]]()

    if (dynamicPartitionOverwrite) {
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p     = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        val old   = listDataFileNames(partFs, p)
        if (old.nonEmpty) {
          removedByPartition.put(partDir, old.asJava)
          logInfo(s"ManifestCommitProtocol: captured ${old.size} old files " +
            s"from $partDir before dynamic overwrite")
        }
      }
    } else {
      // Static: use what deleteWithJob() recorded
      pendingDeleteByDir.forEach { (dirPath, oldFiles) =>
        if (!oldFiles.isEmpty) removedByPartition.put(dirPath, oldFiles)
      }
    }

    // ── Step 3: super.commitJob() ─────────────────────────────────────────
    //
    // DYNAMIC overwrite (what super does):
    //   for each partition in partitionPaths (from Tuple2._2):
    //     fs.delete(outputPath/p=1, recursive=true)   ← DELETES old dir entirely
    //     fs.rename(stagingDir/p=1, outputPath/p=1)   ← moves new files to final
    //   fs.delete(stagingDir)
    //
    //   Result: outputPath/p=1/ has ONLY new data files after this call.
    //   Any _started_ or _committed_ we wrote at task time is now gone
    //   (it was inside the old dir that got deleted). This is why we must
    //   write _committed_ AFTER this call, not before.
    //
    // STATIC overwrite (what super does):
    //   Renames staged files to final output dir.
    //   Old files still coexist with new files (we deferred deletion).
    //   Result: outputPath/p=1/ has OLD + NEW data files after this call.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: Write per-partition _committed_ ───────────────────────────
    //
    // Written AFTER super.commitJob() for two reasons:
    //   (a) Dynamic: partition dirs now exist at final location with new files.
    //       Writing _committed_ before would have it deleted by super's
    //       fs.delete(finalPartDir, true).
    //   (b) Static: new files are now at final location (alongside old ones).
    //       _committed_ correctly lists only the new files; old files will be
    //       deleted in step 6 below.
    mergedPartitionFiles.forEach { (partDir, addedFiles) =>
      val partPath = new Path(partDir)
      val partFs   = partPath.getFileSystem(jobContext.getConfiguration)
      val removed  = removedByPartition.getOrDefault(
        partDir, new java.util.ArrayList[String]())
      writePartitionCommitted(
        partFs, partPath, addedFiles.asScala.toSeq, removed.asScala.toSeq)
    }

    // ── Step 5: Write root-level _committed_ ─────────────────────────────
    val allRemoved = removedByPartition.values().asScala
      .flatMap(_.asScala).toSeq
    writeRootCommitted(jobFs, outputDir, mergedPartitionFiles, allRemoved)

    // ── Step 6: Delete old files (STATIC overwrite only) ─────────────────
    //
    // Delete ONLY the specific OLD files captured in deleteWithJob().
    // NOT the whole directory — that would destroy new data files and
    // the _committed_ manifest we just wrote in step 4.
    //
    // After this step:
    //   outputPath/p=1/ has: new data files + _committed_<tid>
    //   Old data files are gone.
    if (!dynamicPartitionOverwrite) {
      pendingDeleteByDir.forEach { (dirPath, oldFileNames) =>
        val dirFs = new Path(dirPath).getFileSystem(jobContext.getConfiguration)
        oldFileNames.forEach { name =>
          val filePath = new Path(dirPath, name)
          try {
            if (dirFs.exists(filePath)) {
              dirFs.delete(filePath, false)
              logDebug(s"ManifestCommitProtocol: deleted old file $filePath")
            }
          } catch {
            case e: Exception =>
              logWarning(
                s"ManifestCommitProtocol: could not delete old file $filePath: ${e.getMessage}")
          }
        }
      }
      pendingDeleteByDir.clear()
    }

    // ── Step 7: Delete _started_ files ───────────────────────────────────
    //
    // Root _started_ — always present
    safeDelete(jobFs, new Path(outputDir, s"_started_$jobId"))

    if (dynamicPartitionOverwrite) {
      // For DYNAMIC: partition-level _started_ files were inside the old
      // partition dirs. super.commitJob() deleted those dirs entirely in step 3,
      // so _started_ is already gone. Nothing to do for partition level.
      //
      // But clean up any _started_/_committed_ from PREVIOUS jobs that might
      // be lingering in the newly renamed partition dirs.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p     = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifests(p, partFs)
      }
    } else {
      // For STATIC: partition-level _started_ still exists (dir was not deleted).
      // Delete it per partition.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p     = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        safeDelete(partFs, new Path(p, s"_started_$jobId"))
      }
    }

    logInfo(
      s"ManifestCommitProtocol.commitJob: complete. " +
        s"path=$outputPath partitions=${mergedPartitionFiles.size()} " +
        s"dynamic=$dynamicPartitionOverwrite")
  }

  override def abortJob(jobContext: JobContext): Unit = {
    // Discard pending deletes — old data is preserved on failure.
    pendingDeleteByDir.clear()
    super.abortJob(jobContext)
    // Leave _started_ in place — signals failed/incomplete write to readers.
    logInfo(s"ManifestCommitProtocol.abortJob: old data preserved at $outputPath")
  }

  // ═══════════════════════════════════════════════════════════════════════════
  //  TASK LIFECYCLE — EXECUTOR
  //  The committer is deserialized here — @transient fields are NULL.
  //  setupTask() is the ONLY safe initialization point for executor state.
  // ═══════════════════════════════════════════════════════════════════════════

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    super.setupTask(taskContext)
    // Initialize here, NOT inline. Inline initialization is wiped by
    // Java deserialization because the fields are @transient.
    taskPartitionFiles = new java.util.concurrent.ConcurrentHashMap()
    seenPartitionDirs  = new java.util.concurrent.ConcurrentHashMap()
  }

  // ─────────────────────────────────────────────────────────────────────────
  //  newTaskTempFile — override the FileNameSpec variant (Spark 3.5)
  //
  //  The deprecated (ext: String) variant is NEVER called by Spark 3.5 for
  //  partitioned writes. If you only override that one, this body never fires:
  //    → _started_ never written at partition level
  //    → taskPartitionFiles stays empty
  //    → _committed_ has no file entries
  // ─────────────────────────────────────────────────────────────────────────
  override def newTaskTempFile(
                                taskContext: TaskAttemptContext,
                                dir: Option[String],
                                spec: FileNameSpec): String = {

    ensureTaskState(taskContext)

    // Let super handle path construction and staging routing
    val stagingPath = super.newTaskTempFile(taskContext, dir, spec)

    // Compute FINAL destination dir (not staging):
    //   Partitioned:   outputPath/year=2024/month=01
    //   Unpartitioned: outputPath
    val partDir = dir match {
      case Some(d) => new Path(outputPath, d).toString
      case None    => outputPath
    }

    // Write partition-level _started_ the first time a file is written
    // to this partition (one _started_ per partition per task).
    if (seenPartitionDirs.putIfAbsent(partDir, java.lang.Boolean.TRUE) == null) {
      val partPath = new Path(partDir)
      val taskFs   = partPath.getFileSystem(taskContext.getConfiguration)
      cleanupOrphanedStarted(partPath, taskFs)
      touchFile(taskFs, new Path(partPath, s"_started_$jobId"))
      logDebug(s"ManifestCommitProtocol: wrote _started_ at $partDir " +
        s"task=${taskContext.getTaskAttemptID}")
    }

    // Track bare filename (same before and after staging→final rename)
    // under the FINAL partition dir for use in _committed_ manifest.
    taskPartitionFiles
      .computeIfAbsent(partDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)

    stagingPath   // return super's staging path unchanged
  }

  override def newTaskTempFileAbsPath(
                                       taskContext: TaskAttemptContext,
                                       absoluteDir: String,
                                       spec: FileNameSpec): String = {

    ensureTaskState(taskContext)
    val stagingPath = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    taskPartitionFiles
      .computeIfAbsent(absoluteDir, _ => new java.util.concurrent.CopyOnWriteArrayList())
      .add(new Path(stagingPath).getName)
    stagingPath
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    ensureTaskState(taskContext)

    // super.commitTask() does Hadoop staging rename and returns:
    //   TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
    // where partitionPaths (for dynamic overwrite) = Set("year=2024/month=01", ...)
    // We MUST preserve this Tuple2 inside CombinedCommitPayload.superObj so
    // super.commitJob() can correctly cast it and handle partition renames/deletes.
    val superMsg = super.commitTask(taskContext)

    // Snapshot partition→files map for this task
    val snapshot = new java.util.HashMap[String, java.util.List[String]]()
    taskPartitionFiles.forEach { (partDir, files) =>
      snapshot.put(partDir, new java.util.ArrayList[String](files))
    }

    logDebug(
      s"ManifestCommitProtocol.commitTask: " +
        s"partitions=${snapshot.size()} " +
        s"files=${snapshot.values().asScala.map(_.size()).sum} " +
        s"task=${taskContext.getTaskAttemptID}")

    // Clear executor-side state after snapshotting
    taskPartitionFiles.clear()
    seenPartitionDirs.clear()

    new TaskCommitMessage(CombinedCommitPayloadV2(superMsg.obj, snapshot))
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles != null) taskPartitionFiles.clear()
    if (seenPartitionDirs  != null) seenPartitionDirs.clear()
    super.abortTask(taskContext)
    // Leave partition-level _started_ as failure signal to readers
  }

  // ── Guard ─────────────────────────────────────────────────────────────────
  private def ensureTaskState(taskContext: TaskAttemptContext): Unit = {
    if (taskPartitionFiles == null || seenPartitionDirs == null) {
      logWarning(
        "ManifestCommitProtocol: executor state is null — calling setupTask() as recovery. " +
          s"task=${taskContext.getTaskAttemptID}")
      setupTask(taskContext)
    }
  }

  // ── Manifest writers ──────────────────────────────────────────────────────

  private def writePartitionCommitted(
                                       partFs:  FileSystem,
                                       partDir: Path,
                                       added:   Seq[String],
                                       removed: Seq[String]): Unit = {

    val addedJson   = added.map(f => s""""$f"""").mkString(",")
    val removedJson = removed.map(f => s""""$f"""").mkString(",")
    val json        = s"""{"added":[$addedJson],"removed":[$removedJson]}"""
    writeJson(partFs, new Path(partDir, s"_committed_$jobId"), json)
    logDebug(s"ManifestCommitProtocol: wrote _committed_ at $partDir " +
      s"added=${added.size} removed=${removed.size}")
  }

  private def writeRootCommitted(
                                  rootFs:         FileSystem,
                                  rootDir:        Path,
                                  partitionFiles: java.util.HashMap[String, java.util.List[String]],
                                  allRemoved:     Seq[String]): Unit = {

    // Build relative-path "added" list for root manifest
    val addedRelative = partitionFiles.asScala.flatMap { case (partDir, files) =>
      val rel = partDir.stripPrefix(outputPath).stripPrefix("/")
      files.asScala.map(f => if (rel.isEmpty) f else s"$rel/$f")
    }.toSeq

    // Build partitions map in root manifest
    val partitionsJson = partitionFiles.asScala.map { case (partDir, files) =>
      val rel  = partDir.stripPrefix(outputPath).stripPrefix("/")
      val fStr = files.asScala.map(f => s""""$f"""").mkString(",")
      s""""$rel":[$fStr]"""
    }.mkString(",")

    val addedStr   = addedRelative.map(f => s""""$f"""").mkString(",")
    val removedStr = allRemoved.map(f => s""""$f"""").mkString(",")

    val json = s"""{"added":[$addedStr],"removed":[$removedStr],"partitions":{$partitionsJson}}"""
    writeJson(rootFs, new Path(rootDir, s"_committed_$jobId"), json)
  }

  // ── Utilities ─────────────────────────────────────────────────────────────

  private def touchFile(fs: FileSystem, path: Path): Unit = {
    val out = fs.create(path, true); out.close()
  }

  private def writeJson(fs: FileSystem, path: Path, json: String): Unit = {
    val out = fs.create(path, true)
    try { out.write(json.getBytes("UTF-8")) } finally { out.close() }
  }

  private def safeDelete(fs: FileSystem, path: Path): Unit =
    try { fs.delete(path, false) }
    catch { case e: Exception =>
      logWarning(s"ManifestCommitProtocol: could not delete $path: ${e.getMessage}") }

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
}
//```
//
//---
//
//## The two root causes — complete diagnosis
//  ```
//DYNAMIC OVERWRITE bug — _committed_ gets deleted, old data "not deleted" (actually it was):
//
//  Our old code:                          What actually happened:
//  ──────────────────────────────────     ─────────────────────────────────────
//    Step 1: write _committed_ to p=1/      _committed_ written to outputPath/p=1/
//  Step 2: super.commitJob()              fs.delete(outputPath/p=1/, true)
//  ↑ deletes EVERYTHING including _committed_!
//  fs.rename(stagingDir/p=1/, outputPath/p=1/)
//  ↑ staging had no _committed_
//  Result: old data IS deleted (by super),
//  but _committed_ is also gone → reader sees NoManifest → reads raw listing
//
//  Fix: write _committed_ AFTER super.commitJob()
//
//
//  STATIC OVERWRITE bug — new data deleted along with old:
//
//    Our old code:                          What actually happened:
//    ──────────────────────────────────     ─────────────────────────────────────
//      deleteWithJob(): pendingDeletes += path (whole dir e.g. outputPath/p=1/)
//  super.commitJob(): rename staging → outputPath/p=1/ (new files now here)
//  write _committed_ to outputPath/p=1/
//    Step 4: fs.delete(outputPath/p=1/, true)
//  ↑ deletes EVERYTHING: old files + new files + _committed_!
//
//  Fix: in deleteWithJob(), capture OLD filenames by listing now.
//  In commitJob(), delete only those specific old files by filename,
//  NOT the whole directory.
//
//
//  REMOVED metric bug:
//
//    Dynamic: old files were deleted by super before we could list them.
//  Fix: list partition dirs BEFORE super.commitJob() in step 2.
//
//  Static: deleteWithJob() was called with path only, old filenames not captured.
//  Fix: list old files in deleteWithJob() and store them in pendingDeleteByDir.
//class ManifestFileCommitProtocolV2 {
//
//}

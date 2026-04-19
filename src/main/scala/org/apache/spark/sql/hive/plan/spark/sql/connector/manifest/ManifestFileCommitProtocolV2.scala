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
    // path = table root for full overwrite, or specific prefix for partial overwrite.
    // Recurse to find every partition leaf dir that directly contains data files.
    // Also record the exact top-level path passed in so we can delete empty parent
    // dirs later if needed.
    collectOldFilesRecursively(fs, path)
    logInfo(
      s"ManifestCommitProtocol.deleteWithJob: deferred $path → " +
        s"${pendingDeleteByDir.size()} partition dir(s) with old data")
    true
  }


  private def collectOldFilesRecursively(fs: FileSystem, dir: Path): Unit = {
    val statuses =
      try {
        fs.listStatus(dir)
      }
      catch {
        case _: Exception => return
      }

    val (subDirs, files) = statuses.partition(_.isDirectory)

    // Data files directly in this dir (not hidden)
    val dataFilesHere = files
      .map(_.getPath.getName)
      .filter(isDataFile)

    if (dataFilesHere.nonEmpty) {
      // This is a partition leaf dir (or root for unpartitioned).
      // Key MUST be qualified — matches newTaskTempFile's makeQualified key.
      val qualifiedKey = fs.makeQualified(dir).toString
      val list = new java.util.ArrayList[String]()
      dataFilesHere.foreach(list.add)
      pendingDeleteByDir.put(qualifiedKey, list)
      logDebug(
        s"ManifestCommitProtocol.collectOldFiles: $qualifiedKey → " +
          s"${dataFilesHere.length} old file(s)")
    }

    // Recurse into non-hidden subdirs (partition dirs: p=1/, year=2024/, etc.)
    subDirs
      .filterNot(s => {
        val n = s.getPath.getName
        n.startsWith("_") || n.startsWith(".")
      })
      .foreach(s => collectOldFilesRecursively(fs, s.getPath))
  }

  private def cleanupEmptyAncestors(
                                     fs: FileSystem,
                                     dir: Path,
                                     stopAt: Path): Unit = {

    // Qualify both paths so string comparison is reliable
    val qualDir = try {
      fs.makeQualified(dir)
    }
    catch {
      case _: Exception => return
    }
    val qualStopAt = try {
      fs.makeQualified(stopAt)
    }
    catch {
      case _: Exception => return
    }

    // Never climb above or to the table root
    if (qualDir == qualStopAt) return
    if (!qualDir.toString.startsWith(qualStopAt.toString)) return

    try {
      if (!fs.exists(qualDir)) return

      val children = fs.listStatus(qualDir)

      // Check if this dir is truly empty — no files, no subdirs
      // (hidden files like _started_ that we may have left are NOT counted
      //  as "real" content; we check for data files and non-hidden subdirs)
      val hasRealContent = children.exists { s =>
        val name = s.getPath.getName
        !name.startsWith("_") && !name.startsWith(".")
      }

      if (!hasRealContent) {
        // Dir is empty (or only has hidden marker files) — delete it
        fs.delete(qualDir, /* recursive = */ true)
        logInfo(
          s"ManifestCommitProtocol: deleted empty ancestor dir $qualDir")

        // Continue walking up
        cleanupEmptyAncestors(fs, qualDir.getParent, qualStopAt)
      }
      // else: dir still has real content (siblings) → stop climbing

    } catch {
      case e: Exception =>
        logWarning(
          s"ManifestCommitProtocol: cleanupEmptyAncestors failed at $qualDir: " +
            s"${e.getMessage}")
      // Do not propagate — ancestor cleanup is best-effort
    }
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[TaskCommitMessage]): Unit = {

    // ── Step 1: unwrap CombinedCommitPayload ────────────────────────────────
    val mergedPartitionFiles =
      new java.util.HashMap[String, java.util.List[String]]()

    val superMessages: Seq[TaskCommitMessage] = taskCommits.map { msg =>
      msg.obj match {
        case cp: CombinedCommitPayload =>
          cp.partitionFiles.forEach { (partDir, files) =>
            mergedPartitionFiles
              .computeIfAbsent(partDir, _ => new java.util.ArrayList[String]())
              .addAll(files)
          }
          new TaskCommitMessage(cp.superObj)
        case _ => msg
      }
    }

    // ── Step 2: capture REMOVED lists BEFORE super.commitJob() ──────────────
    //
    // Dynamic overwrite: list old files in each written partition dir NOW,
    //   before super.commitJob() deletes them.
    // Static overwrite: already captured in deleteWithJob → collectOldFilesRecursively.
    val removedByPartition =
    new java.util.HashMap[String, java.util.List[String]]()

    val newFileSet = mergedPartitionFiles.asScala.flatMap(f => f._2.asScala).toSet
    if (dynamicPartitionOverwrite) {
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p = new Path(partDir)
        val partFs = p.getFileSystem(jobContext.getConfiguration)
        val old = listDataFileNames(partFs, partFs.makeQualified(p)).filterNot(newFileSet.contains)
        if (old.nonEmpty) {
          removedByPartition.put(partDir, old.asJava)
          logInfo(s"ManifestCommitProtocol: captured ${old.size} old file(s) at $partDir")
        }
      }
    } else {
      // Static: pendingDeleteByDir has old file lists keyed by qualified path.
      // Build removedByPartition from only the partitions that ARE being written
      // (others get the full dir deleted below, not listed in manifest).
      pendingDeleteByDir.forEach { (qualDir, oldFiles) =>
        if (mergedPartitionFiles.containsKey(qualDir) && !oldFiles.isEmpty) {
          removedByPartition.put(qualDir, oldFiles)
        }
      }
      // For partitions NOT in mergedPartitionFiles, the whole dir will be deleted
      // in step 6. We still record their files in the ROOT manifest removed list.
      pendingDeleteByDir.forEach { (qualDir, oldFiles) =>
        if (!mergedPartitionFiles.containsKey(qualDir) && !oldFiles.isEmpty) {
          // These files are going away via full dir delete; add to removed list
          removedByPartition.merge(
            qualDir, new java.util.ArrayList[String](oldFiles),
            (a, b) => {
              a.addAll(b); a
            })
        }
      }
    }

    // ── Step 3: super.commitJob() ────────────────────────────────────────────
    // Dynamic: deletes old partition dirs, renames staging → final.
    // Static:  renames staged files to final (old files still coexist).
    // _committed_ MUST be written AFTER this call in both cases.
    super.commitJob(jobContext, superMessages)

    // ── Step 4: write per-partition _committed_ (AFTER super) ────────────────
    mergedPartitionFiles.forEach { (partDir, addedFiles) =>
      val partPath = new Path(partDir)
      val partFs = partPath.getFileSystem(jobContext.getConfiguration)
      val removed = removedByPartition.getOrDefault(
        partDir, new java.util.ArrayList[String]())
      writePartitionCommitted(
        partFs, partPath, addedFiles.asScala.toSeq, removed.asScala.toSeq)
    }

    // ── Step 5: write root-level _committed_ ─────────────────────────────────
    val allRemoved = removedByPartition.values().asScala.flatMap(_.asScala).toSeq
    writeRootCommitted(jobFs, outputDir, mergedPartitionFiles, allRemoved)

    // ── Step 6: clean up old data (STATIC overwrite only) ────────────────────
    //
    //  TWO DIFFERENT ACTIONS depending on whether a partition got new data:
    //
    //  A) Partition IS in mergedPartitionFiles (e.g. p=1, p=2):
    //     New data files + _committed_ are already in this dir.
    //     Delete only the specific OLD data files by name.
    //     Do NOT delete the dir or _committed_.
    //
    //  B) Partition is NOT in mergedPartitionFiles (e.g. p=3):
    //     No new data was written here. The partition must be fully removed.
    //     Delete the ENTIRE directory recursively.
    //     This is the bug fix: old code only deleted files, leaving an empty
    //     orphan directory that still appeared in partition discovery.
    //
    if (!dynamicPartitionOverwrite) {
      pendingDeleteByDir.forEach { (qualDir, oldFileNames) =>
        val dirPath = new Path(qualDir)
        val dirFs = dirPath.getFileSystem(jobContext.getConfiguration)

        if (mergedPartitionFiles.containsKey(qualDir)) {
          // ── Case A: partition has new data ──────────────────────────────────
          // Delete only the old data files captured before the write.
          // Leave new data files and _committed_ untouched.
          oldFileNames.forEach { name =>
            val filePath = new Path(qualDir, name)
            try {
              if (dirFs.exists(filePath)) {
                dirFs.delete(filePath, false)
                logDebug(s"ManifestCommitProtocol: deleted old file $filePath")
              }
            } catch {
              case e: Exception =>
                logWarning(
                  s"ManifestCommitProtocol: could not delete old file $filePath: " +
                    s"${e.getMessage}")
            }
          }

        } else {
          // ── Case B: partition has NO new data ───────────────────────────────
          // This partition does not exist in the new dataset.
          // Delete the entire partition directory so it is completely removed
          // from the table. Leaving an empty dir would cause it to appear in
          // partition discovery (inferPartitioning) with zero rows.
          try {
            if (dirFs.exists(dirPath)) {
              dirFs.delete(dirPath, /* recursive = */ true)
              logInfo(
                s"ManifestCommitProtocol: deleted stale partition dir $qualDir " +
                  s"(not present in new write)")
              cleanupEmptyAncestors(dirFs, dirPath.getParent, new Path(outputPath))
            }
          } catch {
            case e: Exception =>
              logWarning(
                s"ManifestCommitProtocol: could not delete stale partition dir " +
                  s"$qualDir: ${e.getMessage}")
          }
        }
      }
      pendingDeleteByDir.clear()
    }

    // ── Step 7: delete _started_ files ───────────────────────────────────────
    safeDelete(jobFs, new Path(outputDir, s"_started_$jobId"))

    if (dynamicPartitionOverwrite) {
      // Dynamic: super deleted old partition dirs entirely (step 3).
      // Partition-level _started_ is gone with them. Clean old manifests only.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        cleanupOldManifests(p, pFs)
      }
    } else {
      // Static: dirs for written partitions still exist. Delete their _started_.
      // Dirs for stale partitions (case B) were deleted entirely in step 6.
      mergedPartitionFiles.keySet().forEach { partDir =>
        val p = new Path(partDir)
        val pFs = p.getFileSystem(jobContext.getConfiguration)
        safeDelete(pFs, new Path(p, s"_started_$jobId"))
      }
    }

    logInfo(
      s"ManifestCommitProtocol.commitJob: complete. " +
        s"path=$outputPath " +
        s"partitions_written=${mergedPartitionFiles.size()} " +
        s"partitions_deleted_entirely=${
          pendingDeleteByDir.asScala.keys
            .count(k => !mergedPartitionFiles.containsKey(k))
        } " +
        s"dynamic=$dynamicPartitionOverwrite " +
        s"added=${mergedPartitionFiles.values().asScala.map(_.size()).sum} " +
        s"removed=${allRemoved.size}")
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

    new TaskCommitMessage(CombinedCommitPayload(superMsg.obj, snapshot))
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

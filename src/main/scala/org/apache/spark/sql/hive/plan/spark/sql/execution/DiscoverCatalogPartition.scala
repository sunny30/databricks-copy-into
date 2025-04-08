package org.apache.spark.sql.hive.plan.spark.sql.execution

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName
import org.apache.spark.sql.delta.SerializableFileStatus
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.delta.util.DeltaFileOperations.defaultHiddenFileFilter
import org.apache.spark.sql.types.DataType

import java.io.FileNotFoundException
import java.util.{Locale, TimeZone}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object DiscoverCatalogPartition {


  def listPartitionDirRecurse(dir: String): Iterator[SerializableFileStatus] = {
    val logStore = getLogStore(SparkSession.active)
    val conf = getHadoopConf(SparkSession.active)
    val listOfDirs = list(dir = dir, tries = 10, logStore,
      hadoopConf = conf, hiddenFileNameFilter = defaultHiddenFileFilter).toSeq
    /*    log.info("currently going to traverse recursively the partition folder {}", listOfDirs.head.getHadoopPath.toString)*/
    if (listOfDirs.nonEmpty) {
      listOfDirs.flatMap(d =>
      recurseDirectory(d, tries = 10, logStore,
        hadoopConf = conf, hiddenFileNameFilter = defaultHiddenFileFilter)
      ).iterator
    } else {
      Iterator.empty
    }

  }


  def recurseDirectory(dir: SerializableFileStatus,
                       tries: Int,
                       logStore: LogStore,
                       hadoopConf: Configuration,
                       recurse: Boolean = true,
                       hiddenFileNameFilter: String => Boolean,
                       listAsDirectories: Boolean = true): Iterator[SerializableFileStatus] = {
    dir match {
      case d: SerializableFileStatus if d.isDir =>
        val listOfDirs = list(d.path, tries, logStore, hadoopConf, recurse, hiddenFileNameFilter, listAsDirectories).toSeq
        if (listOfDirs.nonEmpty) {
          listOfDirs.flatMap(d =>
          recurseDirectory(d, tries, logStore, hadoopConf, recurse, hiddenFileNameFilter, listAsDirectories)
          ).iterator
        } else {
          Iterator.single(dir)
        }
      case _ => Iterator.single(dir)
    }

  }

  def getHadoopConf(spark: SparkSession): Configuration = {
    spark.sessionState.newHadoopConf()
  }

  def getLogStore(sparkSession: SparkSession): LogStore = {
    val logStore = LogStore(sparkSession)
    logStore
  }

  def list(dir: String,
           tries: Int,
           logStore: LogStore,
           hadoopConf: Configuration,
           recurse: Boolean = true,
           hiddenFileNameFilter: String => Boolean,
           listAsDirectories: Boolean = true): Iterator[SerializableFileStatus] = {
    try {

      val path = if (listAsDirectories) new Path(dir, "\u0000") else new Path(dir + "\u0000")
      logStore.listFrom(path, hadoopConf)
        .filterNot(f => hiddenFileNameFilter(f.getPath.getName) || !f.isDirectory)
        .map(SerializableFileStatus.fromStatus)
    } catch {
      case NonFatal(e)   =>
     //   randomBackoff("listing", e)
        list(dir, tries = 1, logStore, hadoopConf, recurse, hiddenFileNameFilter, listAsDirectories)
      case e: FileNotFoundException =>
        Iterator.empty
    }
  }


  def detectPartitionFromSinglePath(partitionPath: Path, basePaths: Set[Path]): (Option[PartitionValues], Option[Path]) = {

    val dateFormatter = DateFormatter()
    val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)


    parsePartition(
      partitionPath,
      typeInference = true,
      basePaths,
      userSpecifiedDataTypes = Map.empty)
  }


  def parsePartition(
                      path: Path,
                      typeInference: Boolean,
                      basePaths: Set[Path],
                      userSpecifiedDataTypes: Map[String, DataType],
                      validatePartitionColumns: Boolean=false): (Option[PartitionValues], Option[Path]) = {
    val columns = ArrayBuffer.empty[(String, String)]
    var finished = path.getParent == null
    var currentPath: Path = path

    while (!finished) {
      if (currentPath.getName.toLowerCase(Locale.ROOT) == "_temporary") {
        return (None, None)
      }

      if (basePaths.contains(currentPath)) {
        finished = true
      } else {
        // Let's say currentPath is a path of "/table/a=1/", currentPath.getName will give us a=1.
        // Once we get the string, we try to parse it and find the partition column and value.
        val maybeColumn =
        parsePartitionColumn(currentPath.getName)
        maybeColumn.foreach(columns += _)

        // Now, we determine if we should stop.
        // When we hit any of the following cases, we will stop
        finished =
          (maybeColumn.isEmpty && columns.nonEmpty) || currentPath.getParent == null

        if (!finished) {
          // For the above example, currentPath will be "/table/".
          currentPath = currentPath.getParent
        }
      }
    }

    if (columns.isEmpty) {
      (None, Some(path))
    } else {
      val (columnNames, values) = columns.reverse.unzip
      (Some(PartitionValues(columnNames.toSeq, values.toSeq)), Some(currentPath))
    }
  }


  def parsePartitionColumn(columnSpec: String): Option[(String, String)] = {
    val equalSignIndex = columnSpec.indexOf('=')
    if (equalSignIndex == -1) {
      None
    } else {
      val columnName = unescapePathName(columnSpec.take(equalSignIndex))
      assert(columnName.nonEmpty, s"Empty partition column name in '$columnSpec'")

      val rawColumnValue = columnSpec.drop(equalSignIndex + 1)
      assert(rawColumnValue.nonEmpty, s"Empty partition column value in '$columnSpec'")


      Some(columnName -> rawColumnValue)
    }
  }

  case class PartitionValues(columnNames: Seq[String], values: Seq[String]) {
    require(columnNames.size == values.size)

    def prepareMap: Map[String, String] = {
      columnNames.zip(values).toMap
    }
  }
}


class DiscoverCatalogPartition extends Logging
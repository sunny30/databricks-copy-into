package org.apache.spark.sql.hive.datashare


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.delta.SerializableFileStatus
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.convert.ConvertUtils.timestampPartitionPattern
import org.apache.spark.sql.delta.commands.convert.{ConvertTargetFile, ConvertUtils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{DateFormatter, DeltaFileOperations, PartitionUtils, TimestampFormatter}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.util.Locale
import scala.util.{Random, Try}
import scala.util.control.NonFatal
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.hive.datashare.DataSharePartitionUtils.PartitionValues
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}


object FSUtils {

  var numFiles: Long = 0

  private val log = LoggerFactory.getLogger(classOf[FSUtils])


  def defaultHiddenFileFilter(fileName: String): Boolean = {
    fileName.startsWith("_") || fileName.startsWith(".")
  }

  def listPartitionDirRecurse(dir: String): Iterator[SerializableFileStatus] = {
    val logStore = getLogStore(SparkSession.active)
    val conf = FSUtils.getHadoopConf(SparkSession.active)
    val listOfDirs = list(dir = dir, tries = 10, logStore,
      hadoopConf = conf, hiddenFileNameFilter = defaultHiddenFileFilter).toSeq
    /*    log.info("currently going to traverse recursively the partition folder {}", listOfDirs.head.getHadoopPath.toString)*/
    if (listOfDirs.nonEmpty) {
      recurseDirectory(listOfDirs.head, tries = 10, logStore,
        hadoopConf = conf, hiddenFileNameFilter = FSUtils.defaultHiddenFileFilter)
    } else {
      Iterator.empty
    }

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
      case NonFatal(e) if isThrottlingError(e) && tries > 0 =>
        randomBackoff("listing", e)
        list(dir, tries - 1, logStore, hadoopConf, recurse, hiddenFileNameFilter, listAsDirectories)
      case e: FileNotFoundException =>
        Iterator.empty
    }
  }

  /*
  * listing all the directory from one directory,
  * and traversing one of the directory recursively with the head of list directory
  * */
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
          recurseDirectory(listOfDirs.head, tries, logStore, hadoopConf, recurse, hiddenFileNameFilter, listAsDirectories)
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

  private def randomBackoff(
                             opName: String,
                             t: Throwable,
                             base: Int = 100,
                             jitter: Int = 1000): Unit = {
    val sleepTime = Random.nextInt(jitter) + base
    Thread.sleep(sleepTime)
  }

  private def isThrottlingError(t: Throwable): Boolean = {
    Option(t.getMessage).exists(_.toLowerCase(Locale.ROOT).contains("slow down"))
  }

  protected lazy val serializableConf: SerializableConfiguration = {
    // scalastyle:off deltahadoopconfiguration
    new SerializableConfiguration(SparkSession.active.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }


  protected def doList(basePath: String): Dataset[SerializableFileStatus] = {
    val spark = SparkSession.active
    val conf = SparkSession.active.sparkContext.broadcast(serializableConf)
    DeltaFileOperations
      .recursiveListDirs(spark, Seq(basePath), conf, ConvertUtils.dirNameFilter)
      .where("!isDir")
  }


  //all data files have uniform schema in BDF
  def allFiles(basePath: String, schema: Option[StructType] = None): Dataset[ConvertTargetFile] = {
    val schemaDefined = schema.isDefined
    val files = doList(basePath).mapPartitions { iter => {
      val fileStatuses = iter.toSeq
      fileStatuses.map { fileStatus =>
        if (schemaDefined) {
          ConvertTargetFile(
            fileStatus,
            None,
            Some(schema.get.toDDL))
        } else {
          ConvertTargetFile(
            fileStatus,
            None,
            None)
        }
      }.toIterator
    }
    }

    files.cache()
    files
  }


  def getPartitionValues(file: SerializableFileStatus, basePath: Path): Map[String, String] = {
    val path = file.getHadoopPath
    val pathStr = file.getHadoopPath.toUri.toString
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)

    val (partitionOpt, _) = DataSharePartitionUtils.parsePartition(
      path,
      typeInference = false,
      basePaths = Set(basePath),
      userSpecifiedDataTypes = Map.empty,
      validatePartitionColumns = false,
      java.util.TimeZone.getDefault,
      dateFormatter,
      timestampFormatter)

    val conf = SparkSession.active.sqlContext.conf
    val tz = Option(conf.sessionLocalTimeZone)
    if (partitionOpt.isDefined) {
      val res = partitionOpt.map { partValues =>


        (partValues.columnNames, partValues.literals).zipped.map((pk, pv) => {
          (pk, pv.toString())
        }).toMap
      }
      res.get
    } else {
      Map.empty[String, String]
    }


  }

  def createAddFile(
                     targetFile: ConvertTargetFile,
                     basePath: Path,
                     fs: FileSystem,
                     conf: SQLConf,
                     partitionSchema: Option[StructType],
                     useAbsolutePath: Boolean = false): AddFile = {

    numFiles = numFiles + 1
    val file = targetFile.fileStatus
    val path = file.getHadoopPath

    val partition = getPartitionValues(file, basePath)

    val pathStrForAddFile = if (!useAbsolutePath) {
      val relativePath = DeltaFileOperations.tryRelativizePath(fs, basePath, path)
      assert(!relativePath.isAbsolute,
        s"Fail to relativize path $path against base path $basePath.")
      relativePath.toUri.toString
    } else {
      path.toUri.toString
    }

    AddFile(pathStrForAddFile, partition, file.length, file.modificationTime, dataChange = true)


  }


}


class FSUtils extends Logging


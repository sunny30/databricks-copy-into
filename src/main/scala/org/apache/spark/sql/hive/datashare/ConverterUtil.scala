package org.apache.spark.sql.hive.datashare


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.commands.convert.ConvertTargetFileManifest
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.delta.actions.Format
import org.apache.spark.sql.hive.datashare.DeltashareConsts.{delta_log, last_checkpoint, parquet}

import scala.collection.JavaConverters.asScalaIteratorConverter

case class ConverterUtil(basePath: Option[Path], table: Option[CatalogTable], format: String) extends DeltaLogging {

  def getFormat: Format = {
    Format(
      provider = format
    )
  }

  def generateDeltaLog(sparkSession: SparkSession, tablePath: String, format: String): Seq[Row] = {
    val deltaPathToUse = new Path(tablePath)
    val deltaLog = DeltaLog.forTable(sparkSession, deltaPathToUse)
    val schema = if (table.isDefined) {
      table.get.schema
    } else {
      format.toLowerCase match {
        case "csv" => sparkSession.read.format(format).option("header", "true").load(tablePath).schema
        case "json" => sparkSession.read.format(format).option("multiLine", true).load(tablePath).schema
        case _ => sparkSession.read.format(format).load(tablePath).schema
      }
    }

    val partitionColumnNames = if (table.isDefined) {
      table.get.partitionColumnNames
    } else {
      DataSharePartitionUtils.detectPartitionColumnName(tablePath)
    }

    val partitionSchema = if (table.isDefined) {
      table.get.partitionSchema
    } else {
      StructType(
        DataSharePartitionUtils.getInferSchemaWithPartition(schema, partitionColumnNames).filter(p => p.isPartition).map(p =>
          StructField(p.columnName, DataType.fromDDL(p.datatype))
        )
      )
    }


    val txn = deltaLog.startTransaction()
    performConvert(tablePath, format, schema, Some(partitionSchema), sparkSession, txn)
  }

  def performConvert(path: String,
                     format: String,
                     schema: StructType,
                     partitionSchema: Option[StructType],
                     spark: SparkSession,
                     txn: OptimisticTransaction): Seq[Row] = {

    recordDeltaOperation(txn.deltaLog, "delta.convert") {
      txn.deltaLog.ensureLogDirectoryExist()
      val targetPath = new Path(path)
      val sessionHadoopConf = spark.sessionState.newHadoopConf()
      val fs = targetPath.getFileSystem(sessionHadoopConf)
      val partitionFields = partitionSchema
      val metadata = Metadata(
        schemaString = schema.json,
        partitionColumns = partitionFields.getOrElse(StructType(Seq.empty[StructField])).fieldNames,
        format = getFormat,
        createdTime = Some(System.currentTimeMillis()))
      txn.updateMetadataForNewTable(metadata
      )
      val addFilesIter = createDeltaActions(
        spark,
        partitionSchema.getOrElse(StructType(Seq.empty[StructField])),
        txn,
        fs,
        schema
      )
      val numFiles = FSUtils.numFiles
      val metrics = Map[String, String](
        "numConvertedFiles" -> numFiles.toString
      )

      val operation = DeltaOperations.Convert(
        numFiles,
        partitionSchema.map(_.fieldNames.toSeq).getOrElse(Nil),
        collectStats = false,
        None,
        sourceFormat = Some(format))

      val (committedVersion, postCommitSnapshot) = txn.commitLarge(
        spark,
        Iterator.single(txn.protocol) ++ addFilesIter,
        operation,
        Map.empty,
        metrics)

      // Delete the check point and parquet file for now which is causing issue for DeltaLog standalone read issue
      // with delta share server. And also for now this is not being used.
      val fsStatus = fs.listStatus(new Path(path + delta_log))
      fsStatus.foreach(fileStatus => {
        if (fileStatus.getPath.getName.endsWith(parquet)
          || fileStatus.getPath.getName.endsWith(last_checkpoint)) {
          fs.delete(fileStatus.getPath)
        }
      })
    }

    Seq.empty[Row]

  }


  protected def createDeltaActions(
                                    spark: SparkSession,
                                    partitionSchema: StructType,
                                    txn: OptimisticTransaction,
                                    fs: FileSystem,
                                    schema: StructType
                                  ): Iterator[AddFile] = {

    val shouldCollectStats = false
    val conf = SparkSession.active.sqlContext.conf
    val statsBatchSize = conf.getConf(DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_STATS_COLLECTION)

    val basePath = txn.deltaLog.dataPath
    FSUtils.allFiles(basePath.toUri.toString, Some(schema)).
      toLocalIterator().asScala.grouped(statsBatchSize).flatMap { batch =>
        val adds = batch.map(
          FSUtils.createAddFile(
            _, txn.deltaLog.dataPath, fs, conf, Some(partitionSchema)))

        adds.toIterator
      }
  }


}
package org.apache.spark.sql.arrow

import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.arrow.ArrowFileFormat.UnsafeItr
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.net.URLDecoder
import scala.collection.JavaConverters._

class ArrowFileFormat extends FileFormat with DataSourceRegister with Serializable {


  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String], path: Path): Boolean = {
    ArrowUtils.isOriginalFormatSplitable(
      new ArrowOptions(new CaseInsensitiveStringMap(options.asJava).asScala.toMap))
  }

  def convert(files: Seq[FileStatus], options: Map[String, String]): Option[StructType] = {
    ArrowUtils.readSchema(files, new CaseInsensitiveStringMap(options.asJava))
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {


    convert(files, options)
  }


  def readParquetIter(dataSetFactory: FileSystemDatasetFactory, options: ScanOptions): Seq[ArrowReader] = {
    try {
      val dataSet = dataSetFactory.finish()
      val scanner = dataSet.newScan(options)
      scanner.scan().iterator().asScala.map(it => it.execute()).toSeq
      //val readers = scanner.scan().iterator().asScala.map(it => it.execute())

    } catch {
      case e: Exception => throw e
    }
  }

  def getContent(reader: ArrowReader): Unit = {

    // var fields:Seq[List[FieldVector]] = Seq.empty[List[FieldVector]]
    while (reader.loadNextBatch()) {
      try {
        val vectors = reader.getVectorSchemaRoot.getFieldVectors
        // val tbl: ArrowTa
        vectors
        //  fields = fields++vectors.asScala.toList[FieldVector]
      } catch {
        case e: Exception => throw e
      }
    }


  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] ={

    val sqlConf = sparkSession.sessionState.conf;
    val batchSize = sqlConf.parquetVectorizedReaderBatchSize
 //   val enableFilterPushDown = sqlConf.arrowFilterPushDown


    (file: PartitionedFile) => {
      val factory = ArrowUtils.makeArrowDiscovery(
        file.filePath.toString, file.start, file.length,
        null)

//      new ArrowOptions(
//        new CaseInsensitiveStringMap(
//          options.asJava).asScala.toMap)

   //   val dataset = factory.finish();


      //      val filter = if (enableFilterPushDown) {
      //        ArrowFilters.translateFilters(filters)
      //      } else {
      //        org.apache.arrow.dataset.filter.Filter.EMPTY
      //      }

      val scanOption = new ScanOptions(32768)

     // val scanner = dataset.newScan(scanOption)


      val readers = readParquetIter(dataSetFactory = factory, scanOption)
      readers.foreach(r => getContent(r))
      val itrList = readers.map(r => r.getVectorSchemaRoot)

//      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => {
//        itrList.foreach(_.close())
//        taskList.foreach(_.close())
//        scanner.close()
//        dataset.close()
//        factory.close()
//      }))

      val itr = itrList
        .toIterator
        .map(vsr => ArrowUtils.loadVectors(vsr, file.partitionValues, partitionSchema,
          requiredSchema))
      new UnsafeItr(itr).asInstanceOf[Iterator[InternalRow]]
    }


  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for Arrow source")
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = true

  override def shortName(): String = "arrow"



}


object ArrowFileFormat {
  class UnsafeItr[T](delegate: Iterator[ColumnarBatch])
    extends Iterator[ColumnarBatch] {
    val holder = new ColumnarBatchRetainer()

    override def hasNext: Boolean = {
      holder.release()
      val hasNext = delegate.hasNext
      hasNext
    }

    override def next(): ColumnarBatch = {
      val b = delegate.next()
      holder.retain(b)
      b
    }
  }

  class ColumnarBatchRetainer {
    private var retained: Option[ColumnarBatch] = None

    def retain(batch: ColumnarBatch): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }
}

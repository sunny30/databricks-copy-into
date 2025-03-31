package org.apache.spark.sql.arrow

import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.jni
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.source.DatasetFactory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.dataset.file._
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector}

import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter, asScalaIteratorConverter}

object ArrowTest {

  def getFactory(path: String, format: String): FileSystemDatasetFactory ={
    val scanOption = new ScanOptions(32768)
    try{
      val allocator = new RootAllocator()
      val datasetFactory = new FileSystemDatasetFactory(allocator,NativeMemoryPool.getDefault(),
      FileFormat.PARQUET,
      path)
      datasetFactory
    }catch {
      case e:Exception => throw e
    }
  }


  def readParquet(dataSetFactory: FileSystemDatasetFactory, options:ScanOptions):ArrowReader={
    try {
      val dataSet = dataSetFactory.finish()
      val scanner = dataSet.newScan(options)
      val reader = scanner.scanBatches()
      reader
//      reader.getVectorSchemaRoot.getVector(0).
      //val readers = scanner.scan().iterator().asScala.map(it => it.execute())

    }catch {
      case e:Exception =>throw e
    }
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

  def getParquetContent(reader: ArrowReader):Unit={

   // var fields:Seq[List[FieldVector]] = Seq.empty[List[FieldVector]]
    while(reader.loadNextBatch()){
      try{
        val vectors = reader.getVectorSchemaRoot.getFieldVectors
       // val tbl: ArrowTa
         vectors
      //  fields = fields++vectors.asScala.toList[FieldVector]
      }catch {
        case e: Exception => throw e
      }
    }


  }

  def prepColumnVector(reader: ArrowReader):Array[ColumnVector]={

    reader.getVectorSchemaRoot.
      getFieldVectors.asScala.
      map(vec => new ArrowColumnVector(vec)).toArray
  }

  def prepColumnVector(root: VectorSchemaRoot): Array[ColumnVector] = {
    root.
      getFieldVectors.asScala.
      map(vec => new ArrowColumnVector(vec)).toArray
  }

  def loadArrow(fileString:String):Unit = {
    val file = SparkPath.fromPathString(fileString)
    val factory = ArrowUtils.makeArrowDiscovery(
      file.toString,0, 0,
      null)

    //      new ArrowOptions(
    //        new CaseInsensitiveStringMap(
    //          options.asJava).asScala.toMap)

    //val dataset = factory.finish();


    //      val filter = if (enableFilterPushDown) {
    //        ArrowFilters.translateFilters(filters)
    //      } else {
    //        org.apache.arrow.dataset.filter.Filter.EMPTY
    //      }

    val scanOption = new ScanOptions(32768)
    //val scanner = dataset.newScan(scanOption)


    val readers = readParquetIter(dataSetFactory = factory, scanOption)
   // val readers = scanner.scan().iterator().asScala.map(it => it.execute()).toSeq
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
      .map(vsr => ArrowUtils.loadVectors(vsr, null, null,
        null))
  }


  def main(args: Array[String]):Unit={
    val path = "file:///tmp/parquet/part-00001-59dc62c0-f6db-4b72-84d4-a08a4ac205b9-c000.snappy.parquet"
    //file:///tmp/parquet/part-00001-59dc62c0-f6db-4b72-84d4-a08a4ac205b9-c000.snappy.parquet
    val factory = ArrowUtils.makeArrowDiscovery(path, 0, 0 , null)
    val scanOption = new ScanOptions(32768)
    val readers = readParquetIter(factory, scanOption)
    readers.foreach(r=>getParquetContent(r))
    val columns = readers.map(r => prepColumnVector(r.getVectorSchemaRoot))
//    columns.foreach(c => c.toList)
   // loadArrow(path)

  }

}

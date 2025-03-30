package org.apache.spark.sql.arrow

import org.apache.arrow.dataset.file.FileSystemDatasetFactory
import org.apache.arrow.dataset.jni
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.dataset.source.DatasetFactory
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.dataset.file._
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.ipc.ArrowReader

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

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
         vectors
      //  fields = fields++vectors.asScala.toList[FieldVector]
      }catch {
        case e: Exception => throw e
      }
    }


  }


  def main(args: Array[String]):Unit={
    val path = "file:///Users/sharadsingh/Dev/databricks-copy-into/spark-warehouse/cat.cat/tdb1.db/tbl/part-00000-ae10be1d-0689-4e33-83f9-4e0060f3e4b4-c000.snappy.parquet"
    val factory = getFactory(path, "parquet")
    val scanOption = new ScanOptions(32768)
    val readers = readParquetIter(factory, scanOption)
    readers.foreach(r=>getParquetContent(r))

  }

}

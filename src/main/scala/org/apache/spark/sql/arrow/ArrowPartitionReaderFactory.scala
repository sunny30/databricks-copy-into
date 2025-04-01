package org.apache.spark.sql.arrow

import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.arrow.ArrowPartitionReaderFactory.ColumnarBatchRetainer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URLDecoder
import scala.collection.JavaConverters.asScalaIteratorConverter

case class ArrowPartitionReaderFactory(
                                        sqlConf: SQLConf,
                                        broadcastedConf: Broadcast[SerializableConfiguration],
                                        readDataSchema: StructType,
                                        readPartitionSchema: StructType,
                                        pushedFilters: Array[Filter],
                                        options: ArrowOptions,
                                        filters: Seq[Filter])
  extends FilePartitionReaderFactory {

  private val batchSize = sqlConf.parquetVectorizedReaderBatchSize
  private val enableFilterPushDown: Boolean = false

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    // disable row based read
    throw new UnsupportedOperationException
  }

  override def buildColumnarReader(
                                    partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val path = partitionedFile.filePath
    val factory = ArrowUtils.makeArrowDiscovery(partitionedFile.filePath.toPath.toString,
      partitionedFile.start, partitionedFile.length, options)
    val dataset = factory.finish()

    val scanOptions = new ScanOptions(readDataSchema.map(f => f.name).toArray, batchSize)
    val scanner = dataset.newScan(scanOptions)

    val taskList = scanner
      .scan()
      .iterator()
      .asScala
      .toList

//    val vsrItrList = taskList
//      .map(task => task.scan())

    val itrList = taskList
      .map(task => task.execute().getVectorSchemaRoot)

    val batchItr = itrList
      .toIterator
      .map(vsr => ArrowUtils.loadVectors(vsr, partitionedFile.partitionValues, readPartitionSchema,
        readDataSchema))
    new PartitionReader[ColumnarBatch] {
      val holder = new ColumnarBatchRetainer()

      override def next(): Boolean = {
        holder.release()
        batchItr.hasNext
      }

      override def get(): ColumnarBatch = {
        val batch = batchItr.next()
        holder.retain(batch)
        batch
      }

      override def close(): Unit = {
        holder.release()
       // vsrItrList.foreach(itr => itr.close())
        taskList.foreach(task => task.close())
        scanner.close()
        dataset.close()
        factory.close()
      }
    }
  }
}

object ArrowPartitionReaderFactory {
  private class ColumnarBatchRetainer {
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

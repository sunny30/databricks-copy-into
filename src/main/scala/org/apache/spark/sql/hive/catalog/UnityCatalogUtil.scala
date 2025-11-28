package org.apache.spark.sql.hive.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, TableIdentifier}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.execution.command.PartitionStatistics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.duration.MILLISECONDS
import scala.reflect.runtime.universe.TypeTag


class UnityCatalogUtil(spark:SparkSession) extends Logging {

  def listColumns(catalogName:String, dbName:String, tableName:String):Dataset[Column]={

    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName))
//    val v2table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName)) match {
//      case v2Table: V2Table => v2Table.v1Table
//      case _ => throw new IllegalArgumentException("only v2 is allowed")
//    }
    if(table.partitioning() != null) {
      val partitionColumnNames = table.partitioning.toSeq.convertTransforms
      val bucketColumnNames = Nil

      val columns = schemaToColumns(table.schema(), partitionColumnNames._1.contains, bucketColumnNames.contains)
      makeDataset(columns, spark)
    }else{
      val columns =  schemaToColumns(table.schema())
      makeDataset(columns, spark)
    }
  }


  private def schemaToColumns(
                               schema: StructType,
                               isPartCol: String => Boolean = _ => false,
                               isBucketCol: String => Boolean = _ => false): Seq[Column] = {
    schema.map { field =>
      new Column(
        name = field.name,
        description = field.getComment().orNull,
        dataType = field.dataType.simpleString,
        nullable = field.nullable,
        isPartition = isPartCol(field.name),
        isBucket = isBucketCol(field.name))
    }
  }

  def makeDataset[T <: DefinedByConstructorParams : TypeTag](
                                                              data: Seq[T],
                                                              sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val toRow = enc.createSerializer()
    val encoded = data.map(d => toRow(d).copy())
    val plan = new LocalRelation(DataTypeUtils.toAttributes(enc.schema), encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](queryExecution, enc)
  }

  def getTableLocation(tableIdent: TableIdentifier):(String,String)={
    val catalogName = tableIdent.catalog.getOrElse("spark_catalog")
    val dbName = tableIdent.database.getOrElse("default")
    val tableName = tableIdent.table
    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName))
    table match {
      case sparkTable: SparkTable => (sparkTable.properties().get("format"), sparkTable.properties().get("location"))
      case v2Table: V2Table => (v2Table.v1Table.provider.getOrElse("parquet"), v2Table.v1Table.storage.locationUri.getOrElse("").toString)
      case _ => throw new IllegalStateException("only V2 or Iceberg table is required")
    }

  }

  def getCatalogTablePartitions(tableIdentifier: TableIdentifier):Seq[CatalogTablePartition] = {
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableName = tableIdentifier.table
    val catalogName = tableIdentifier.catalog.getOrElse(spark.sessionState.catalogManager.currentCatalog.name())
    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName))
    table match {
     // case sparkTable: SparkTable => (sparkTable.properties().get("format"), sparkTable.properties().get("location"))
      case v2Table: V2Table => getCatalogTablePartitions(v2Table)
      case _ => throw new IllegalStateException("only V2 or Iceberg table is required")
    }
  }

  def getV1CatalogTableFromV2Table(tableIdentifier: TableIdentifier): CatalogTable = {
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableName = tableIdentifier.table
    val catalogName = tableIdentifier.catalog.getOrElse(spark.sessionState.catalogManager.currentCatalog.name())
    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = tableCatalog.loadTable(Identifier.of(Array(dbName), tableName))
    table match {
      // case sparkTable: SparkTable => (sparkTable.properties().get("format"), sparkTable.properties().get("location"))
      case v2Table: V2Table => v2Table.v1Table
      case _ => throw new IllegalStateException("only V2 or Iceberg table is required")
    }
  }

  def getCatalogTablePartitions(v2Table: V2Table):Seq[CatalogTablePartition]= {

    val table = v2Table.v1Table
    val root = new Path(table.location)
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fs = root.getFileSystem(hadoopConf)
    val threshold = spark.sparkContext.conf.get(RDD_PARALLEL_LISTING_THRESHOLD)
    val pathFilter = getPathFilter(hadoopConf)
    val evalPool = ThreadUtils.newForkJoinPool("RepairTableCommand", 8)
    val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] =
      try {
        scanPartitions(spark, fs, pathFilter, root, Map(), table.partitionColumnNames, threshold,
          spark.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool)).seq
      } finally {
        evalPool.shutdown()
      }
    val batchSize = spark.conf.get(SQLConf.ADD_PARTITION_BATCH_SIZE)
    var parts = Seq.empty[CatalogTablePartition] ;
    partitionSpecsAndLocs.iterator.grouped(batchSize).foreach { batch =>
      parts = batch.map { case (spec, location) =>
        // inherit table storage format (possibly except for location)
        CatalogTablePartition(
          spec,
          table.storage.copy(locationUri = Some(location.toUri)),
          Map.empty[String, String])
      }

    }
    parts
  }

  private def getPathFilter(hadoopConf: Configuration): PathFilter = {
    // Dummy jobconf to get to the pathFilter defined in configuration
    // It's very expensive to create a JobConf(ClassUtil.findContainingJar() is slow)
    val jobConf = new JobConf(hadoopConf, this.getClass)
    val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
    path: Path => {
      val name = path.getName
      if (name != "_SUCCESS" && name != "_temporary" && !name.startsWith(".")) {
        pathFilter == null || pathFilter.accept(path)
      } else {
        false
      }
    }
  }

  private def scanPartitions(
                              spark: SparkSession,
                              fs: FileSystem,
                              filter: PathFilter,
                              path: Path,
                              spec: TablePartitionSpec,
                              partitionNames: Seq[String],
                              threshold: Int,
                              resolver: Resolver,
                              evalTaskSupport: ForkJoinTaskSupport): Seq[(TablePartitionSpec, Path)] = {
    if (partitionNames.isEmpty) {
      return Seq(spec -> path)
    }

    val statuses = fs.listStatus(path, filter)
    val statusPar: Seq[FileStatus] =
      if (partitionNames.length > 1 && statuses.length > threshold || partitionNames.length > 2) {
        // parallelize the list of partitions here, then we can have better parallelism later.
        val parArray = new ParVector(statuses.toVector)
        parArray.tasksupport = evalTaskSupport
        parArray.seq
      } else {
        statuses
      }
    statusPar.flatMap { st =>
      val name = st.getPath.getName
      if (st.isDirectory && name.contains("=")) {
        val ps = name.split("=", 2)
        val columnName = ExternalCatalogUtils.unescapePathName(ps(0))
        // TODO: Validate the value
        val value = ExternalCatalogUtils.unescapePathName(ps(1))
        if (resolver(columnName, partitionNames.head)) {
          scanPartitions(spark, fs, filter, st.getPath, spec ++ Map(partitionNames.head -> value),
            partitionNames.drop(1), threshold, resolver, evalTaskSupport)
        } else {
          logWarning(
            s"expected partition column ${partitionNames.head}, but got ${ps(0)}, ignoring it")
          Seq.empty
        }
      } else {
        logWarning(s"ignore ${new Path(path, name)}")
        Seq.empty
      }
    }
  }

}

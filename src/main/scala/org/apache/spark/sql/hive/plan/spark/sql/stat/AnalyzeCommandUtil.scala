package org.apache.spark.sql.hive.plan.spark.sql.stat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableSchemaChangeCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.execution.command.{CommandUtils, PartitionStatistics}
import org.apache.spark.sql.execution.command.CommandUtils.{calculateMultipleLocationSizes, calculateSingleLocationSize}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.hive.plan.spark.sql.execution.DiscoverCatalogPartition
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogTablePartition, CatalogTableType, CatalogUtils, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DatetimeType, DecimalType, DoubleType, FloatType, IntegralType, StringType}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

import java.net.URI
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.duration.MILLISECONDS

object AnalyzeCommandUtil extends Logging {

  val NUM_FILES = "numFiles"
  val TOTAL_SIZE = "totalSize"
  val DDL_TIME = "transient_lastDdlTime"


  def analyzeTable(
                    sparkSession: SparkSession,
                    tableIdent: TableIdentifier,
                    plugin: CatalogPlugin): Unit = {
    val table = plugin.asTableCatalog.loadTable(Identifier.of(Seq(tableIdent.database.getOrElse("default")).toArray, tableIdent.table))
    if (table.isInstanceOf[V2Table]) {
      val catalogTable = table.asInstanceOf[V2Table].v1Table
      val tableLocation = catalogTable.storage.locationUri.getOrElse(catalogTable.location)
      val (totalSize:Long, newPartitions: Seq[CatalogTablePartition]) = if (catalogTable.partitionColumnNames.isEmpty) {
        (calculateSingleLocationSize(SparkSession.active.sessionState, catalogTable.identifier,
          catalogTable.storage.locationUri), Seq.empty[CatalogTablePartition])
      } else {
        val root = new Path(catalogTable.location)
        val hadoopConf = SparkSession.active.sessionState.newHadoopConf()
        val fs = root.getFileSystem(hadoopConf)
        val threshold = SparkSession.active.sparkContext.conf.get(RDD_PARALLEL_LISTING_THRESHOLD)
        val pathFilter = getPathFilter(hadoopConf)
        val evalPool = ThreadUtils.newForkJoinPool("RepairTableCommand", 8)
        val partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)] =
          try {
            scanPartitions(sparkSession, fs, pathFilter, root, Map(), catalogTable.partitionColumnNames, threshold,
              sparkSession.sessionState.conf.resolver, new ForkJoinTaskSupport(evalPool)).seq
          } finally {
            evalPool.shutdown()
          }

        val total = partitionSpecsAndLocs.length

        val partitionStats = if (sparkSession.sqlContext.conf.gatherFastStats) {
          gatherPartitionStats(sparkSession, partitionSpecsAndLocs, fs, pathFilter, threshold)
        } else {
          Map.empty[Path, PartitionStatistics]
        }

        logInfo(s"Finished to gather the fast stats for all $total partitions.")

        var discoveredPartitions: Seq[CatalogTablePartition] = Seq.empty[CatalogTablePartition]
        var totalSumSize: Long = 0
        val batchSize = sparkSession.conf.get(SQLConf.ADD_PARTITION_BATCH_SIZE)
        partitionSpecsAndLocs.iterator.grouped(batchSize).foreach { batch =>
          val now = MILLISECONDS.toSeconds(System.currentTimeMillis())
          val partitions = batch.map { case (spec, location) =>
            val params = partitionStats.get(location).map {
              case PartitionStatistics(numFiles, totalSize) =>
                // This two fast stat could prevent Hive metastore to list the files again.
                Map(NUM_FILES -> numFiles.toString,
                  TOTAL_SIZE -> totalSize.toString,
                  // Workaround a bug in HiveMetastore that try to mutate a read-only parameters.
                  // see metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java
                  DDL_TIME -> now.toString)
            }.getOrElse(Map.empty)
            // inherit table storage format (possibly except for location)
            CatalogTablePartition(
              spec,
              catalogTable.storage.copy(locationUri = Some(location.toUri)),
              params)
          }
          totalSumSize = partitionStats.map(p => p._2.totalSize).sum
          discoveredPartitions  = partitions.seq
          //(totalSize, partitions)
        }
        (totalSumSize, discoveredPartitions)
      }

      val qualifiedTableName = tableIdent.catalog.getOrElse("spark_catalog") + "." + tableIdent.database.getOrElse("default") + "." + tableIdent.table
      val rowCount = Some(BigInt(sparkSession.read.table(qualifiedTableName).count()))
      val newStats = CommandUtils.compareAndGetNewStats(catalogTable.stats, totalSize, rowCount)
      plugin.asInstanceOf[TableSchemaChangeCatalog].alterTableStats(tableIdent.database.getOrElse("default"), tableIdent.table, newStats)


    }
  }

  def getPathFilter(hadoopConf: Configuration): PathFilter = {
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


  def scanPartitions(
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


  def gatherPartitionStats(
                            spark: SparkSession,
                            partitionSpecsAndLocs: Seq[(TablePartitionSpec, Path)],
                            fs: FileSystem,
                            pathFilter: PathFilter,
                            threshold: Int): Map[Path, PartitionStatistics] = {
    val partitionNum = partitionSpecsAndLocs.length
    if (partitionNum > threshold) {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val serializableConfiguration = new SerializableConfiguration(hadoopConf)
      val locations = partitionSpecsAndLocs.map(_._2)

      // Set the number of parallelism to prevent following file listing from generating many tasks
      // in case of large #defaultParallelism.
      val numParallelism = Math.min(partitionNum,
        Math.min(spark.sparkContext.defaultParallelism, 10000))
      // gather the fast stats for all the partitions otherwise Hive metastore will list all the
      // files for all the new partitions in sequential way, which is super slow.
      logInfo(s"Gather the fast stats in parallel using $numParallelism tasks.")
      spark.sparkContext.parallelize(locations, numParallelism)
        .mapPartitions { locationsEachPartition =>
          val pathFilter = getPathFilter(serializableConfiguration.value)
          locationsEachPartition.map { location =>
            val fs = location.getFileSystem(serializableConfiguration.value)
            val statuses = fs.listStatus(location, pathFilter)
            (location, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
          }
        }.collectAsMap().toMap
    } else {
      partitionSpecsAndLocs.map { case (_, location) =>
        val statuses = fs.listStatus(location, pathFilter)
        (location, PartitionStatistics(statuses.length, statuses.map(_.getLen).sum))
      }.toMap
    }
  }


  private def analyzeColumnInCatalog(sparkSession: SparkSession, catalogName: String, dbName: String, tableName: String, columnNames: Option[Seq[String]], allColumns: Boolean): Unit = {

    val tableIdent = TableIdentifier(table = tableName, database = Some(dbName), catalog = Some(catalogName) )
    val plugin = sparkSession.sessionState.catalogManager.catalog(catalogName)
    val v2table = plugin.asTableCatalog.loadTable(Identifier.of(Seq(tableIdent.database.getOrElse("default")).toArray, tableIdent.table))
    if(v2table.isInstanceOf[V2Table]) {
      val tableMeta = v2table.asInstanceOf[V2Table].v1Table
      val (sizeInBytes, _) = CommandUtils.calculateTotalSize(sparkSession, tableMeta)
      val relation = sparkSession.read.table(s"""${catalogName}.${dbName}.${tableName}""").logicalPlan
      val columnsToAnalyze = getColumnsToAnalyze(tableIdent, relation, columnNames, allColumns)

      // Compute stats for the computed list of columns.
      val (rowCount, newColStats) =
        CommandUtils.computeColumnStats(sparkSession, relation, columnsToAnalyze)

      val newColCatalogStats = newColStats.map {
        case (attr, columnStat) =>
          attr.name -> columnStat.toCatalogColumnStat(attr.name, attr.dataType)
      }

      // We also update table-level stats in order to keep them consistent with column-level stats.
      val statistics = CatalogStatistics(
        sizeInBytes = sizeInBytes,
        rowCount = Some(rowCount),
        // Newly computed column stats should override the existing ones.
        colStats = tableMeta.stats.map(_.colStats).getOrElse(Map.empty) ++ newColCatalogStats)

      plugin.asInstanceOf[TableSchemaChangeCatalog].alterTableStats(tableIdent.database.getOrElse("default"), tableIdent.table, Some(statistics))
   //   sessionState.catalog.alterTableStats(tableIdent, Some(statistics))
    }

  }


  private def getColumnsToAnalyze(
                                   tableIdent: TableIdentifier,
                                   relation: LogicalPlan,
                                   columnNames: Option[Seq[String]],
                                   allColumns: Boolean = false): Seq[Attribute] = {
    val columnsToAnalyze = if (allColumns) {
      relation.output
    } else {
      columnNames.get.map { col =>
        val exprOption = relation.output.find(attr => SQLConf.get.resolver(attr.name, col))
        exprOption.getOrElse(throw QueryCompilationErrors.columnNotFoundError(col))
      }
    }
    // Make sure the column types are supported for stats gathering.
    columnsToAnalyze.foreach { attr =>
      if (!supportsType(attr.dataType)) {
        throw QueryCompilationErrors.columnTypeNotSupportStatisticsCollectionError(
          attr.name, tableIdent, attr.dataType)
      }
    }
    columnsToAnalyze
  }


  private def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case BinaryType | StringType => true
    case _ => false
  }

}





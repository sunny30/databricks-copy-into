package org.apache.spark.sql.hive.plan.spark.sql.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.generatePartitionPredicateByFilter
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, ExternalCatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileStatusCache, InMemoryFileIndex, PartitionDirectory, PartitionPath, PartitionSpec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, Cast, ExprId, Literal}



class CustomCatalogFileIndex(
                              sparkSession: SparkSession,
                              override val table: CatalogTable,
                              override val sizeInBytes: Long)
  extends CatalogFileIndex(sparkSession = sparkSession, table = table, sizeInBytes = sizeInBytes) {

  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)


  override def listFiles(
                          partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitionFiltersString = partitionFilters.map(e => e.sql).mkString(":")
    val dataFiltersString = dataFilters.map(e => e.sql).mkString("@")
    println(partitionFiltersString)
    println(dataFiltersString)
    filterPartitions(partitionFilters).listFiles(Nil, dataFilters)
  }

 // override def refresh(): Unit = fileStatusCache.invalidateAll()

  /**
   * Returns a [[InMemoryFileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  override def filterPartitions(filters: Seq[Expression]): InMemoryFileIndex = {
    if (table.partitionColumnNames.nonEmpty) {


      val startTime = System.nanoTime()


      val paths = DiscoverCatalogPartition.
        listPartitionDirRecurse(table.storage.locationUri.getOrElse(table.location).toString).toSeq.map(fst => fst.getHadoopPath)
      val timeNs = System.nanoTime() - startTime
      val partitionPath = paths.map(p => {
        PartitionPath(
          toRow(partitionSchema, sparkSession.sessionState.conf.sessionLocalTimeZone, p),
          p)
      })
      val partitionSpec = PartitionSpec(partitionSchema, partitionPath)
      new InMemoryFileIndex(sparkSession,
        rootPathsSpecified = paths,
        parameters = Map.empty,
        userSpecifiedSchema = Some(table.partitionSchema),
        fileStatusCache = fileStatusCache,
        userSpecifiedPartitionSpec = Some(partitionSpec),
        metadataOpsTimeNs = Some(timeNs))


    } else {
      new InMemoryFileIndex(sparkSession, rootPaths, parameters = table.storage.properties,
        userSpecifiedSchema = None, fileStatusCache = fileStatusCache)
    }
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  // `CatalogFileIndex` may be a member of `HadoopFsRelation`, `HadoopFsRelation` may be a member
  // of `LogicalRelation`, and `LogicalRelation` may be used as the cache key. So we need to
  // implement `equals` and `hashCode` here, to make it work with cache lookup.
  override def equals(o: Any): Boolean = o match {
    case other: CatalogFileIndex => this.table.identifier == other.table.identifier
    case _ => false
  }

  override def hashCode(): Int = table.identifier.hashCode()


  def listPartitionsByFilter(
                              conf: SQLConf,
                              catalog: SessionCatalog,
                              table: CatalogTable,
                              partitionFilters: Seq[Expression]): Seq[CatalogTablePartition] = {
    if (conf.metastorePartitionPruning) {
      catalog.listPartitionsByFilter(table.identifier, partitionFilters)
    } else {
      ExternalCatalogUtils.prunePartitionsByFilter(table, catalog.listPartitions(table.identifier),
        partitionFilters, conf.sessionLocalTimeZone)
    }
  }

  def prunePartitionsByFilter(
                               catalogTable: CatalogTable,
                               inputPartitions: Seq[CatalogTablePartition],
                               predicates: Seq[Expression],
                               defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    if (predicates.isEmpty) {
      inputPartitions
    } else {
      val partitionSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
        catalogTable.partitionSchema)
      val boundPredicate = generatePartitionPredicateByFilter(catalogTable,
        partitionSchema, predicates)

      inputPartitions.filter { p =>
        boundPredicate.eval(p.toRow(partitionSchema, defaultTimeZoneId))
      }
    }
  }


  def toRow(partitionSchema: StructType, defaultTimeZondId: String, path:Path): InternalRow = {
    val caseInsensitiveProperties = CaseInsensitiveMap(table.storage.properties)
    val timeZoneId = caseInsensitiveProperties.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
    InternalRow.fromSeq(partitionSchema.map { field =>
      val partValue = {
      DiscoverCatalogPartition.
        detectPartitionFromSinglePath(path,
          Set(new Path(table.storage.locationUri.getOrElse(table.location))))._1.get.prepareMap.
        getOrElse(field.name, new IllegalArgumentException("no field present"))
      }
      Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
    })
  }




}

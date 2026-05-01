package org.apache.spark.sql.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.hive.plan.spark.sql.connector.hudi.MultiCatalogHudiTable
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class UnityHudiCatalog(metastore: ExternalCatalog, catalogName: String) extends DeltaLogging {


  def createHudiTable(
                   ident: Identifier,
                   schema: StructType,
                   partitions: Array[Transform],
                   properties: util.Map[String, String],
                   catalogTable:CatalogTable
                 ): Table = {
    val db = ident.namespace()(0)
    val tbl = ident.name()
    if (metastore.tableExists(db, tbl)) throw new TableAlreadyExistsException(ident)

    val props = properties.asScala
    val tableType = resolveTableType(props)
    val basePath = resolveBasePath(ident, props)
    val recordKey = props.getOrElse(
      "hoodie.datasource.write.recordkey.field",
      throw new IllegalArgumentException("hoodie.datasource.write.recordkey.field is required")
    )
    val preCombine = props.get("hoodie.datasource.write.precombine.field")
    val partFields = partitions.map(_.describe()).mkString(",")

    // Phase 1: Hudi timeline init
    // Hudi 1.0.1: HadoopStorageConfiguration wraps hadoop Configuration
    val storageConf = new HadoopStorageConfiguration(SparkSession.active.sessionState.newHadoopConf())
    val mcBuilder = HoodieTableMetaClient.newTableBuilder()
      .setTableType(tableType.name())
      .setTableName(tbl)
      .setRecordKeyFields(recordKey)
      .setPartitionFields(partFields)
      .setPayloadClassName(
        props.getOrElse(
          "hoodie.datasource.write.payload.class",
          "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        )
      )
    preCombine.foreach(mcBuilder.setPreCombineField)
    val metaClient = mcBuilder.initTable(storageConf, basePath)

    // Phase 2: metastore registration
    val finalProps = buildFinalProps(props.toMap, tableType, basePath, recordKey, partFields, schema)
    metastore.createTable(catalogTable,true)

    new MultiCatalogHudiTable(SparkSession.active, ident, metaClient, finalProps, catalogName)
  }

  private def resolveTableType(props: mutable.Map[String, String]): HoodieTableType =
    HoodieTableType.valueOf(
      props.getOrElse(HoodieTableConfig.TYPE.key(), "COPY_ON_WRITE").toUpperCase
    )

  private def resolveBasePath(ident: Identifier, props: mutable.Map[String, String]): String =
    props.getOrElse("location",
      s"${ident.namespace()(0)}.db/${ident.name()}")

  private def buildFinalProps(
                               base: Map[String, String],
                               tableType: HoodieTableType,
                               basePath: String,
                               recordKey: String,
                               partitionFields: String,
                               schema: StructType
                             ): Map[String, String] = base ++ Map(
    "provider" -> "hudi",
    "location" -> basePath,
    HoodieTableConfig.TYPE.key() -> tableType.name(),
    "hoodie.datasource.write.recordkey.field" -> recordKey,
    "hoodie.datasource.write.partitionpath.field" -> partitionFields,
    "spark.sql.sources.schema" -> schema.json
  )


  def loadTable(ident: Identifier): Table = {
    val db = ident.namespace()(0)
    val tbl = ident.name()
    if (!metastore.tableExists(db, tbl)) throw new NoSuchTableException(ident)
    val meta = metastore.getTable(db, tbl)
    val mc = loadMetaClient(meta.location.toString)
    new MultiCatalogHudiTable(SparkSession.active, ident, mc, meta.properties, catalogName)
  }

  private def loadMetaClient(basePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder()
      .setConf(new HadoopStorageConfiguration(SparkSession.active.sessionState.newHadoopConf()))
      .setBasePath(basePath) // takes String — confirmed from source
      .setLoadActiveTimelineOnLoad(true)
      .build()


}

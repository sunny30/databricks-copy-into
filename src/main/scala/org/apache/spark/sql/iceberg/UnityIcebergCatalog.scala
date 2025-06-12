package org.apache.spark.sql.iceberg

import org.apache.iceberg.hadoop.{UnityHadoopCatalog, UnitySparkCatalog}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.types.StructType
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.regex.Pattern
class UnityIcebergCatalog(plugin: ExternalCatalog, catalogName: String,options: CaseInsensitiveStringMap) extends DeltaLogging{

  lazy val icebergCatalog = {
    val ct = new UnitySparkCatalog()
    ct.initialize(catalogName, catalogOptions(catalogName, SQLConf.get))
    ct
  }

  def createIcebergTable(
                          ident: Identifier,
                          schema: StructType,
                          partitions: Array[Transform],
                          allTableProperties: java.util.Map[String, String],
                          catalogTable: CatalogTable
                        ): Table ={
    val ct = new SparkCatalog()

    val icebergCatalog = new UnitySparkCatalog()
    val initializedCatalog = icebergCatalog.initialize(catalogName, catalogOptions(catalogName, SQLConf.get))
    val icebergTable = icebergCatalog.createTable(ident, schema,partitions,allTableProperties)

    plugin.createTable(catalogTable, true)
    icebergTable

  }


  private def catalogOptions(name: String, conf: SQLConf) = {
    val prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)")
    val options = new util.HashMap[String, String]
    conf.getAllConfs.foreach {
      case (key, value) =>
        val matcher = prefix.matcher(key)
        if (matcher.matches && matcher.groupCount > 0) options.put(matcher.group(1), value)
    }
    new CaseInsensitiveStringMap(options)
  }


  def loadTable(ident: Identifier): Table = {
    val icebergTable = icebergCatalog.loadTable(ident)
    icebergTable
  }



}

package org.apache.spark.sql.hive.plan.spark.sql.connector.custom

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVDataSourceV2, CSVTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class CustomTable() extends Table with SupportsRead{

  var tablesSchema:StructType = _

  def this(schema: StructType, properties: java.util.Map[String, String]){
    this()
  //  this(catalogTable)
    this.tablesSchema = schema
  }

  override def name(): String = "CustomV2"

  override def schema(): StructType = tablesSchema

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.allOf(classOf[TableCapability])

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CSVDataSourceV2().getTable(tablesSchema,Array.empty[Transform],options).asInstanceOf[CSVTable].newScanBuilder(options)
}

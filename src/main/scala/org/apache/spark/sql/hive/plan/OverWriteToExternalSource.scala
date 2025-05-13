package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.StructType
import org.iq80.leveldb.WriteOptions

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.convert.ImplicitConversions.`map AsScala`

object OverWriteToExternalSource {

  def createAndOverWritePlan(query: LogicalPlan, catalog: CatalogPlugin, ident: Identifier, props: Map[String, String], provider: String, writeOptions: Map[String, String],parts: Seq[Transform]): LogicalPlan = {
    val tableExists = catalog.asTableCatalog.tableExists(ident)
    if (tableExists) {
      catalog.asTableCatalog.dropTable(ident)
    }
    val schema = query.schema
    val optionsMap = props.filter(f => f._1.startsWith(TableCatalog.OPTION_PREFIX))
    val tblProperties = props ++ getpersistOptionsForExternalSource(ident.namespace().head,ident.name())
    catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray, mapAsJavaMap(tblProperties))
    val table = catalog.asTableCatalog.loadTable(ident)
    val catalogTable = table.asInstanceOf[V2Table].v1Table
    val dataSource = DataSource(
      SparkSession.active,
      // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
      // inferred at runtime. We should still support it.
      userSpecifiedSchema = Some(schema),
      className = provider,
      options = catalogTable.storage.properties++ optionsMap ++ writeOptions ++ getOverWriteOptionsForExternalSource(tableExists,ident.namespace().head,ident.name()),
      catalogTable = Some(catalogTable)
    )
    val columnNames = catalogTable.schema.fieldNames
    val relation = LogicalRelation(dataSource.resolveRelation(false), catalogTable)
    InsertIntoStatement(relation, Map.empty[String, Option[String]], columnNames, query, true, false)


  }


  def getOverWriteOptionsForExternalSource(tableExists: Boolean, schemaName:String, tableName: String): Map[String, String] = {
    val res = if (!tableExists) {
      Map(
        "schema" -> s"${schemaName}",
        "table" -> s"$tableName",
        "source.external.catalog" -> "true",
        "write.mode" -> "CREATE"
      )
    } else {
      Map(
        "source.external.catalog" -> "true",
        "write.mode" -> "OverWrite"
      )
    }
    res
  }


  def getpersistOptionsForExternalSource(schemaName:String, tableName: String): Map[String, String] = {
    Map(
      "schema" -> s"${schemaName}",
      "table" -> s"$tableName",
      "option.schema" -> s"${schemaName}",
      "option.table" -> s"$tableName",
      "provider" -> "custom",
      "option.provider" -> "custom"
    )

  }
}

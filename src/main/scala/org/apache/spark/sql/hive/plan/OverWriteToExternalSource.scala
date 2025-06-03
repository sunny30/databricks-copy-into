package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.types.StructType
import org.iq80.leveldb.WriteOptions

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.convert.ImplicitConversions.`map AsScala`

object OverWriteToExternalSource {

  def createAndOverWritePlan(query: LogicalPlan, catalog: CatalogPlugin, ident: Identifier, props: Map[String, String], provider: String, writeOptions: Map[String, String],parts: Seq[Transform]): LogicalPlan = {
    val tableExists = catalog.asTableCatalog.tableExists(ident)
    var externalCatalogTable = false
    if (tableExists) {
      catalog.asTableCatalog.loadTable(ident) match {
        case dt: DeltaTableV2 => catalog.asTableCatalog.dropTable(ident)
        case v2:V2Table => if(v2.v1Table.provider.isDefined){
          if(!v2.v1Table.provider.get.equalsIgnoreCase("custom")){
            catalog.asTableCatalog.dropTable(ident)
          }else{
            externalCatalogTable = true
          }
        }
      }

    }
    val schema = query.schema
    val optionsMap = props.filter(f => f._1.startsWith(TableCatalog.OPTION_PREFIX))

    val namespaceName = if(props.contains("schema") || props.contains("option.schema")){
      if(props.contains("schema")){
        props("schema")
      }else{
        props("option.schema")
      }
    }else{
      ident.namespace().head
    }

    val tableName = if (props.contains("table") || props.contains("option.table")) {
      if (props.contains("table")) {
        props("table")
      } else {
        props("option.table")
      }
    } else {
      ident.name()
    }

    val tblProperties = props ++ getpersistOptionsForExternalSource(namespaceName,tableName)
    if(!externalCatalogTable)
      catalog.asTableCatalog.createTable(ident, query.schema, parts.toArray, mapAsJavaMap(tblProperties))
    val table = catalog.asTableCatalog.loadTable(ident)
    val catalogTable = table.asInstanceOf[V2Table].v1Table
    val dataSource = DataSource(
      SparkSession.active,
      // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
      // inferred at runtime. We should still support it.
      userSpecifiedSchema = Some(schema),
      className = provider,
      options = catalogTable.storage.properties++ optionsMap ++ writeOptions ++ getOverWriteOptionsForExternalSource(tableExists,namespaceName,tableName),
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

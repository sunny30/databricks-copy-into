package org.apache.spark.sql.hive.plan

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.TableNameContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Table, TableSchemaChangeCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.connector.{V2CustomTable, V2Table}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object CLSUtils {

  def getSecureDataSource(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case ds@DataSourceV2Relation(table, output, catalog, identifier, options) => getSecurePlanFromDataSourceV2(ds, table)
      case lr@LogicalRelation(relation, output, catalogTable, isStreaming) if catalogTable.isDefined => getSecurePlanFromLogicalRelation(lr, catalogTable.get)
      case _ => plan

    }
  }

  //covers Iceberg and V2Table
  def getSecurePlanFromDataSourceV2(ds: DataSourceV2Relation, table: Table): LogicalPlan = {
    val (catalogName, dbName, tableName) = getCatalogTableDetails(table)
    val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
    getSecureLeafPlan(secureTable, ds)
  }

  def getSecurePlanFromLogicalRelation(ds: LogicalRelation, table: CatalogTable): LogicalPlan = {
    val (catalogName, dbName, tableName) = (table.identifier.catalog.getOrElse("default"), table.identifier.database.getOrElse("default"), table.identifier.table)
    val secureTable = getSecureTableFrom(catalogName, dbName, tableName)
    getSecureLeafPlan(secureTable, ds)
  }


  def getCatalogTableDetails(table: Table): (String, String, String) = {
    val ct = table match {
      case v2CustomTable: V2CustomTable =>
        v2CustomTable.catalogTable

      case v2Table: V2Table => v2Table.v1Table
      // (ct.identifier.catalog.getOrElse("default"),ct.identifier.database.getOrElse("default"), ct.identifier.table)
      case deltaTableV2: DeltaTableV2 if deltaTableV2.catalogTable.isDefined => deltaTableV2.catalogTable.get
      case sparkTable: SparkTable =>
        val multiPartName = sparkTable.name().split("\\.").toArray
        if (multiPartName.length == 3) {
          val plugin = SparkSession.active.sessionState.catalogManager.catalog(multiPartName(0))
          plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(multiPartName(1), multiPartName(2))
        } else {
          null //bad code needs to be fixed.
        }
      case _ =>
        val multiPartName = table.name().split("\\.").toArray
        if (multiPartName.length == 3) {
          val plugin = SparkSession.active.sessionState.catalogManager.catalog(multiPartName(0))
          plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(multiPartName(1), multiPartName(2))
        } else {
          null //bad code needs to be fixed.
        }


    }

    (ct.identifier.catalog.getOrElse("default"), ct.identifier.database.getOrElse("default"), ct.identifier.table)

  }


  def getSecureTableFrom(catalogName: String, db: String, table: String): CatalogTable = {
    val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
    val ct = plugin.asInstanceOf[TableSchemaChangeCatalog].loadSecureTable(db, table)
    ct
  }

  def getSecureLeafPlan(catalogTable: CatalogTable, leafPlan: LogicalPlan): LogicalPlan = {

    if (leafPlan.getTagValue(TreeNodeTag[String]("cls-sec")).isEmpty) {
      val secureFields = catalogTable.schema.fields.map(f => f.name).toSet
      val secureAttributes = leafPlan.output.filter(at => secureFields.contains(at.name))
      leafPlan.setTagValue(TreeNodeTag[String]("cls-sec"), "cls-sec")
      val analyzed = SparkSession.active.sessionState.analyzer.execute(Project(secureAttributes, leafPlan))
      //analyzed.foreach(pl => pl.setTagValue(TreeNodeTag[String]("cls-sec"), "cls-sec"))
      analyzed
    } else {
      leafPlan
    }
  }

  def getSecureColumns(multipartIdentifier: Seq[String]):Option[Seq[String]]={

    val catalogName = SparkSession.active.sessionState.catalogManager.currentCatalog.name()
    val res = if (multipartIdentifier.size == 3) {
      (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
    } else if (multipartIdentifier.size == 2) {
      (catalogName, multipartIdentifier(0), multipartIdentifier(1))
    } else {
      (catalogName, "default", multipartIdentifier(0))
    }
    try {
      val ct = getSecureTableFrom(res._1, res._2, res._3)
      Some(ct.schema.fields.map(f => f.name).toSeq)
    }catch {
      case e:Exception => None
    }
  }

  def getProjectedTable(plan:LogicalPlan,ctx: TableNameContext):LogicalPlan={
    if(ctx.identifierReference()!=null){
      val multiParts = ctx.identifierReference().multipartIdentifier().parts.map(x=> x.identifier().toString()).toSeq
      val secureColumns = getSecureColumns(multiParts)
      secureColumns match {
        case Some(cols) =>
          val secureAttributes = cols.map(name => UnresolvedAttribute.apply(name))
          Project(secureAttributes, plan)
        case _ => plan
      }
    }else{
      plan
    }

  }



}

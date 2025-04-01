package org.apache.spark.sql.hive.plan.spark.sql.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed.origin
import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateView, LogicalPlan, RefreshTable, ReplaceTableAsSelect, TableSpec, View}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.hive.plan.spark.sql.execution.NonDefaultCatalogCreateViewCommand
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class MaterializedViewParserPlanUtils(sqlText:String){
  val isMaterializedView = (sqlText.toUpperCase().startsWith("CREATE MATERIALIZED VIEW") || sqlText.toUpperCase().startsWith("REFRESH MATERIALIZED VIEW"))

  def getNewSQLText:String = {
    if(isMaterializedView){
      if(sqlText.toUpperCase.startsWith("CREATE"))
        sqlText.toUpperCase().replaceFirst("CREATE MATERIALIZED VIEW", "CREATE VIEW").toLowerCase()
      else{
        sqlText.toUpperCase().replaceFirst("REFRESH MATERIALIZED VIEW", "REFRESH TABLE").toLowerCase()
      }
    }else{
      sqlText
    }
  }

  def getMaterialisedViewSubstitutedPlan(plan: LogicalPlan):LogicalPlan ={
    if(isMaterializedView){
      val substitutedPlan = plan match {
        case viewPlan: CreateView =>
          val query = viewPlan.query
          val child = viewPlan.child
          val tableSpec = TableSpec(properties = Map.empty[String, String],
            provider = Some("delta"), options = Map.empty[String, String],
            location = None, serde = None, comment = None, external = false)
          CreateTableAsSelect(
            name = child,
            query = query,
            partitioning = Seq.empty[Transform],
            tableSpec = tableSpec,
            writeOptions = Map.empty[String,String],
            ignoreIfExists = false
          )

        case nonDefaultCatalogCreateViewCommand: NonDefaultCatalogCreateViewCommand =>
          val nameParts = nonDefaultCatalogCreateViewCommand.name.nameParts
          val child = UnresolvedIdentifier(nameParts = nameParts)
          val query = nonDefaultCatalogCreateViewCommand.plan
          val queryText = nonDefaultCatalogCreateViewCommand.originalText.get
          val props = nonDefaultCatalogCreateViewCommand.properties ++ Map("view-text"->queryText, "view-type"->"materialized")
          val tableSpec = TableSpec(properties = props,
            provider = Some("delta"), options = Map.empty[String, String],
            location = None, serde = None, comment = None, external = false)
          CreateTableAsSelect(
            name = child,
            query = query,
            partitioning = Seq.empty[Transform],
            tableSpec = tableSpec,
            writeOptions = Map.empty[String, String],
            ignoreIfExists = false
          )

        case refreshTable: RefreshTable  =>
          val nameParts = refreshTable.child.asInstanceOf[UnresolvedTableOrView].multipartIdentifier
          if(nameParts.size == 3){
            val catName = nameParts(0)
            val dbName = nameParts(1)
            val tableName = nameParts(2)
            val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog

            val mvCt = sessionCatalog.loadTable(Identifier.of(Seq(dbName).toArray, tableName))
            val props = mvCt match {
              case v:V2Table => v.v1Table.properties
              case deltaTableV2: DeltaTableV2 => deltaTableV2.properties().asScala
            }
            val query = props.get("view-text")

            if(query.isDefined){
              val queryPlan = getParsedPlan(query.get)
              val tableSpec = TableSpec(properties = props.toMap,
                provider = Some("delta"), options = Map.empty[String, String],
                location = None, serde = None, comment = None, external = false)
              val child = UnresolvedIdentifier(nameParts = nameParts)
              ReplaceTableAsSelect(
                name = child,
                query = queryPlan,
                tableSpec = tableSpec,
                partitioning = Seq.empty[Transform],
                writeOptions = Map.empty[String, String],
                orCreate = true
              )
            }else{
              throw new IllegalArgumentException("SQL txt is missing from view")
            }
          }else{
            throw new IllegalArgumentException("three part name is must")
          }

        case _ => plan
      }
      substitutedPlan

    }else{
      plan
    }
  }


  def getParsedPlan(sqlText: String):LogicalPlan = {
    val parsedPlan = try {
        CurrentOrigin.withOrigin(origin) {
          (new CustomSparkSQLParser()).parseQuery(sqlText)
        }
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewText(sqlText, "")
      }

    parsedPlan
  }


}

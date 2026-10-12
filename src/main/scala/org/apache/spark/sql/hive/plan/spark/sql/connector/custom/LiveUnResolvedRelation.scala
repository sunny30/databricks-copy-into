package org.apache.spark.sql.hive.plan.spark.sql.connector.custom

import org.apache.calcite.jdbc.CalcitePrepare.Query
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnresolvedLeafNode
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed.origin
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.{Identifier, TableSchemaChangeCatalog}
import org.apache.spark.sql.delta.DeltaTableValueFunctions.TableFunctionDescription
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser

case class LiveUnResolvedRelation(sql:String, catalogName:String) extends UnresolvedLeafNode



object LiveCatalogTableValuedFunction{
  val tableFuncDesc: TableFunctionDescription = (
    FunctionIdentifier("remote_query"),
    new ExpressionInfo("com.example.Dummy", "remote_query"),

    (args: Seq[Expression]) => {
      if(args.length!=2){
        throw QueryCompilationErrors.wrongNumArgsError(
          "remote_query", // function name
          Seq(2),args.length)
      }
      try {
        val sql = args.head.eval().toString
        val tableName = args(1).eval().toString
        val catalogName = args.last.eval().toString
        LiveUnResolvedRelation(sql, catalogName)

      }catch {
        case e:Exception => throw e
      }
    }
  )

  def isValidCatalog(catalogName:String):Boolean = {
    try {
      val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
      plugin.asInstanceOf[TableSchemaChangeCatalog].isLiveCatalog(catalogName)
    }catch {
      case e:Exception => false
    }

  }


  def getLogicalRelationFromLiveUnResolvedPlan(liveUnResolvedRelation: LiveUnResolvedRelation):LogicalPlan={
    if(!isValidCatalog(liveUnResolvedRelation.catalogName)){
      throw new IllegalArgumentException("Remote QUery function is only allowed on Live catalog")
    }
    val dataSource = DataSource(
      SparkSession.active,
      // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
      // inferred at runtime. We should still support it.

      className = "custom",
      options = Map(
        "query" -> liveUnResolvedRelation.sql,
        "catalog" -> liveUnResolvedRelation.catalogName
      )
    //  options = table.properties ++ ab.writeOptions.toMap ++ getReadOptionsForExternalSource,

    )

    LogicalRelation(dataSource.resolveRelation(false))
  }
}



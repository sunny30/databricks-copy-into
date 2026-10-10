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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.DeltaTableValueFunctions.TableFunctionDescription
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser

case class LiveUnResolvedRelation(sql:String, tableName: String, catalogName:String) extends UnresolvedLeafNode



object LiveCatalogTableValuedFunction{
  val tableFuncDesc: TableFunctionDescription = (
    FunctionIdentifier("remote_query"),
    new ExpressionInfo("com.example.Dummy", "remote_query"),

    (args: Seq[Expression]) => {
      if(args.length!=3){
        throw QueryCompilationErrors.wrongNumArgsError(
          "remote_query", // function name
          Seq(3),args.length)
      }
      try {
        val sql = args.head.eval().asInstanceOf[String]
        val tableName = args(1).eval().asInstanceOf[String]
        val catalogName = args.last.eval().asInstanceOf[String]
        LiveUnResolvedRelation(sql, tableName, catalogName) // any LogicalPlan
      }catch {
        case e:Exception => throw e
      }
    }
  )

  def getDSV2Relation(liveUnResolvedRelation: LiveUnResolvedRelation):LogicalPlan={
    val catalogName = liveUnResolvedRelation.catalogName
    val tableName = liveUnResolvedRelation.tableName
    val parsedPlan = try {
      CurrentOrigin.withOrigin(origin) {
        (new CustomSparkSQLParser()).parseQuery(liveUnResolvedRelation.sql)
      }
    } catch {
      case _: ParseException => throw new AnalysisException("Invalid text")
      // throw SparkApiShim.invalidViewText(viewText, table.v1Table.qualifiedName)
    }
    val plan = SparkSession.active.sessionState.analyzer.executeAndCheck(parsedPlan, new QueryPlanningTracker())
    val tableCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val mutipartname = tableName.split("\\.").toArray

    val (catalogName1, dbName1, tableName1) = if (mutipartname.length == 2) {
      //extract catalog name from conf
      (catalogName, mutipartname.head, mutipartname.last)
    } else if (mutipartname.size == 3) {
      (mutipartname.head, mutipartname(1), mutipartname.last)
    } else {
      (catalogName, mutipartname.head, mutipartname.last)
    }

    val ident = Identifier.of(Seq(dbName1).toArray, tableName1)
    val catalogTable = tableCatalog.loadTable(ident)
    val dsv2 = DataSourceV2Relation.create(catalogTable, Some(tableCatalog), Some(ident))
    dsv2.copy(output = plan.output.map(a=> a.asInstanceOf[AttributeReference]))
  }
}



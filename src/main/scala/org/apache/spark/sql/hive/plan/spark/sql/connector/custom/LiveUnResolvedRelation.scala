package org.apache.spark.sql.hive.plan.spark.sql.connector.custom

import org.apache.calcite.jdbc.CalcitePrepare.Query
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedLeafNode
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.delta.DeltaTableValueFunctions.TableFunctionDescription
import org.apache.spark.sql.errors.QueryCompilationErrors

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
}



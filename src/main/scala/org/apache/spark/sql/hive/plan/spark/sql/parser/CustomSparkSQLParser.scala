package org.apache.spark.sql.hive.plan.spark.sql.parser

import io.delta.sql.parser.DeltaSqlAstBuilder
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.parser.CustomSqlParser
import org.xbill.DNS.ZoneTransferIn.Delta


class CustomSparkSQLParser extends SparkSqlParser{

  override val astBuilder = new CustomAstBuilder()
  private val deltaSqlAstBuilder = new DeltaSqlAstBuilder()

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText){ parser =>

    val ctx = parser.singleStatement()
    withOrigin(ctx, Some(sqlText))
    astBuilder.visitSingleStatement (ctx) match {

      case plan: LogicalPlan => plan
      case _ => deltaSqlAstBuilder.visit(parser.singleStatement()) match {
        case pl: LogicalPlan => pl
        case _ => throw new IllegalArgumentException("Invalid SQL")
      }
    }


  }



}


object CustomSparkSQLParser extends SparkSqlParser{

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>

    val delegate = new CustomSparkSQLParser()
    new CustomSqlParser(delegate).parse(sqlText) match {
      case plan: LogicalPlan => plan
      case _ => throw new IllegalArgumentException("Invalid SQL")
    }
  }


}

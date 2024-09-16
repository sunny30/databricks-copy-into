package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.parser.{AstBuilder, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlAstBuilder

class CustomAstBuilder extends SparkSqlAstBuilder{

  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): LogicalPlan = super.visitSingleStatement(ctx)
}

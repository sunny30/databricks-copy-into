package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.parser.{AstBuilder, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class CustomAstBuilder extends AstBuilder{

  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): LogicalPlan = super.visitSingleStatement(ctx)
}

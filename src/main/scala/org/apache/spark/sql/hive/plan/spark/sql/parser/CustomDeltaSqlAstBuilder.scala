package org.apache.spark.sql.hive.plan.spark.sql.parser

import io.delta.sql.parser.{DeltaSqlAstBuilder, DeltaSqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import org.apache.spark.sql.hive.plan.spark.sql.delta.commad.CustomConvertToDeltaCommand
import org.apache.spark.sql.types.StructType

class CustomDeltaSqlAstBuilder extends DeltaSqlAstBuilder{

  override def visitConvert(ctx: DeltaSqlBaseParser.ConvertContext): LogicalPlan = {
    val plan = ConvertToDeltaCommand(
      visitTableIdentifier(ctx.table),
      Option(ctx.colTypeList).map(colTypeList => StructType(visitColTypeList(colTypeList))),
      ctx.STATISTICS() == null, None)
    CustomConvertToDeltaCommand(plan)
  }

}

package org.apache.spark.sql.hive.plan.spark.sql.parser

import io.delta.sql.parser.DeltaSqlAstBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.parser.CustomSqlParser
import org.xbill.DNS.ZoneTransferIn.Delta


class CustomSparkSQLParser extends SparkSqlParser{

  override val astBuilder = new CustomAstBuilder()
  private val deltaSqlAstBuilder = new DeltaSqlAstBuilder()

  override def parsePlan(sqlText: String): LogicalPlan = {
    (new CustomDeltaSqlParser(this)).parsePlan(sqlText)
  }


  def parserSparkSQLPlan(sqlText: String): LogicalPlan ={
    val newSQLText = MaterializedViewParserPlanUtils(sqlText).getNewSQLText
    parse(newSQLText){
      parser =>
        val plan = astBuilder.visitSingleStatement(parser.singleStatement())
        if(plan ==null){
          throw new IllegalArgumentException("Invalid SQL")
        }else{
          plan
        }
        val newPlan = MaterializedViewParserPlanUtils(sqlText).getMaterialisedViewSubstitutedPlan(plan)
        newPlan
    }
  }



}


object CustomSparkSQLParser extends SparkSqlParser{

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>

    SparkSession.active.conf.set("spark.sql.catalog.cat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.ecat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.hive", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    val delegate = new CustomSparkSQLParser()
    new CustomSqlParser(delegate).parse(sqlText) match {
      case plan: LogicalPlan => plan
      case _ => throw new IllegalArgumentException("Invalid SQL")
    }
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    SparkSession.active.conf.set("spark.sql.catalog.cat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.ecat", "org.apache.spark.sql.hive.catalog.UnityCatalog")

    SparkSession.active.conf.set("spark.sql.catalog.hive", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    super.parseMultipartIdentifier(sqlText)
  }


}

package org.apache.spark.sql.hive.plan.spark.sql.parser

import io.delta.sql.parser.DeltaSqlAstBuilder
import org.apache.iceberg.spark.ExtendedParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateView, LogicalPlan}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.parser.CustomSqlParser
import org.apache.spark.sql.hive.plan.listener.{CatalogQueryExecutionListener, ListenerUtil}
import org.apache.spark.sql.hive.plan.spark.sql.execution.NonDefaultCatalogCreateViewCommand
import org.apache.spark.sql.types.{DataType, StructType}
import org.xbill.DNS.ZoneTransferIn.Delta

import java.util.{List => JList}
import java.util


class CustomSparkSQLParser extends SparkSqlParser with ExtendedParser{

  override def parseSortOrder(orderString: String): JList[ExtendedParser.RawOrderField] = {
    // Delegate to Iceberg's own parser — it knows how to parse
    // "col ASC NULLS LAST", "zorder(col1, col2)" etc.
    // We instantiate it with `this` as delegate so the chain is preserved.
    new IcebergSparkSqlExtensionsParser(this).parseSortOrder(orderString)
  }

  override val astBuilder = new CustomAstBuilder()
  private val deltaSqlAstBuilder = new DeltaSqlAstBuilder()
  private val icebergSqlParser = new CustomIcebergSqlParser(this)

  override def parsePlan(sqlText: String): LogicalPlan = {
    if(icebergSqlParser.isIcebergCommand(sqlText)){
      icebergSqlParser.parsePlan(sqlText)
    }else if(icebergSqlParser.isIcebergProcedure(sqlText)){
      return new org.apache.spark.sql.catalyst.parser.extensions
      .IcebergSparkSqlExtensionsParser(new SparkSqlParser())
        .parsePlan(sqlText)
    }else {
      (new CustomDeltaSqlParser(this)).parsePlan(sqlText)
    }
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


object CustomSparkSQLParser extends SparkSqlParser with ExtendedParser{

  override def parseSortOrder(orderString: String): java.util.List[ExtendedParser.RawOrderField] =
    new IcebergSparkSqlExtensionsParser(new SparkSqlParser())
      .parseSortOrder(orderString)

  override def parsePlan(sqlText: String): LogicalPlan = {

    SparkSession.active.conf.set("spark.sql.catalog.cat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.ecat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.hive", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    val delegate = new CustomSparkSQLParser()
    val icebergSqlParser = new CustomIcebergSqlParser(delegate)

    if(icebergSqlParser.isIcebergProcedure(sqlText)){
      return new org.apache.spark.sql.catalyst.parser.extensions
      .IcebergSparkSqlExtensionsParser(new SparkSqlParser())
        .parsePlan(sqlText)
    }

    if (icebergSqlParser.isIcebergCommand(sqlText)) {
      return icebergSqlParser.parsePlan(sqlText)
    }

    new CustomSqlParser(delegate).parse(sqlText) match {
      case plan: LogicalPlan =>
        plan match {
          case c: NonDefaultCatalogCreateViewCommand => ListenerUtil.setSQLText(c.plan, sqlText)
          case createTableAsSelect: CreateTableAsSelect => ListenerUtil.setSQLText(createTableAsSelect.query, sqlText)
          case _ =>  ListenerUtil.setSQLText(plan, sqlText)
        }

        plan
        //tranformUnResolvedRelationWithThreePartName(plan)
      case _ => throw new IllegalArgumentException("Invalid SQL")
    }
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {


    SparkSession.active.conf.set("spark.sql.catalog.cat", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    SparkSession.active.conf.set("spark.sql.catalog.ecat", "org.apache.spark.sql.hive.catalog.UnityCatalog")

    SparkSession.active.conf.set("spark.sql.catalog.hive", "org.apache.spark.sql.hive.catalog.UnityCatalog")
    super.parseMultipartIdentifier(sqlText)
  }

  def tranformUnResolvedRelationWithThreePartName(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case u: UnresolvedRelation => getThreePartUnResolvedRelation(u)
      case p: LogicalPlan => p
    }
  }

  def getThreePartUnResolvedRelation(u: UnresolvedRelation): UnresolvedRelation ={
    val sz = u.multipartIdentifier.size
    var catalogName = SparkSession.active.sessionState.catalogManager.currentCatalog.name()
    if(catalogName.equalsIgnoreCase("hive") || catalogName.equalsIgnoreCase("default")){
      catalogName = "spark_catalog"
    }
    if(sz >= 3){
      u
    }else if(sz == 2 ){
      val mp:Seq[String] = Seq(catalogName,u.multipartIdentifier.head, u.multipartIdentifier.last)
      u.copy(multipartIdentifier = mp)
    }else{
      val mp:Seq[String] = Seq(catalogName,"default", u.multipartIdentifier.head)
      u.copy(multipartIdentifier = mp)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    val delegate = new CustomSparkSQLParser()
    delegate.parseExpression(sqlText)
  }


  // ADD
  override def parseDataType(sqlText: String): DataType = {
    val delegate = new CustomSparkSQLParser()
    delegate.parseDataType(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    val delegate = new CustomSparkSQLParser()
    delegate.parseTableSchema(sqlText)
  }


}

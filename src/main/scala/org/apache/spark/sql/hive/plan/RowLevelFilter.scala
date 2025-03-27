package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table

class RowLevelFilter(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{


  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

//    case f@Filter(c, ds:DataSourceV2Relation) =>
//      if(f.getTagValue(TreeNodeTag[String]("row-sec")).isDefined){
//        ds.setTagValue(TreeNodeTag[String]("row-sec"), "ds-row-sec")
//      }
//      f

    case d@DataSourceV2Relation(table: V2Table, output:Seq[AttributeReference], _, _, _) =>
      if(table.v1Table.properties.contains("row_sec_func")) {
        val funcName = table.v1Table.properties.getOrElse("row_sec_func", "none")
        val ct = session.sessionState.catalog.getTableMetadata(TableIdentifier(funcName))
        val conditionString = ct.properties.getOrElse("cond", "")
        val condition = session.sessionState.sqlParser.parseExpression(conditionString)
        val filter = Filter(condition, d)
        filter.setTagValue(TreeNodeTag[String]("row-sec"), "ds-row-sec")
        if (d.getTagValue(TreeNodeTag[String]("row-sec")).isEmpty) {
          d.setTagValue(TreeNodeTag[String]("row-sec"), "ds-row-sec")
          val analyzed = session.sessionState.analyzer.execute(Filter(condition, d))
          analyzed
        } else {
          d
        }
      }else{
        d
      }

    case u@UnresolvedRelation(multipartIdentifier: Seq[String], _, _) =>
      val relation = (new CustomDataSourceAnalyzer(session)).apply(u)
      if(relation.isInstanceOf[View]) {
        if(relation.asInstanceOf[View].desc.properties.contains("row_sec_func")) {
          val viewCt = relation.asInstanceOf[View].desc
          val funcName = viewCt.properties.getOrElse("row_sec_func", "none")
          val ct = session.sessionState.catalog.getTableMetadata(TableIdentifier(funcName))
          val conditionString = ct.properties.getOrElse("cond", "")
          val condition = session.sessionState.sqlParser.parseExpression(conditionString)
          //ffcondition.sql
          val filter = Filter(condition, relation)
          val analyzed = session.sessionState.analyzer.execute(filter)
          analyzed
        }else{
          u
        }
      }else{
        u
      }

    case pl: LogicalPlan => pl

  }

}

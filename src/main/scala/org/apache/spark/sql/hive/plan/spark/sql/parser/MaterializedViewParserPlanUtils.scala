package org.apache.spark.sql.hive.plan.spark.sql.parser

import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateView, LogicalPlan, TableSpec}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hive.plan.spark.sql.execution.NonDefaultCatalogCreateViewCommand

case class MaterializedViewParserPlanUtils(sqlText:String){
  val isMaterializedView = sqlText.toUpperCase().startsWith("CREATE MATERIALIZED VIEW")

  def getNewSQLText:String = {
    if(isMaterializedView){
      sqlText.toUpperCase().replaceFirst("CREATE MATERIALIZED VIEW", "CREATE VIEW")
    }else{
      sqlText
    }
  }

  def getMaterialisedViewSubstitutedPlan(plan: LogicalPlan):LogicalPlan ={
    if(plan.isInstanceOf[CreateView] || plan.isInstanceOf[NonDefaultCatalogCreateViewCommand]){
      val substitutedPlan = plan match {
        case viewPlan: CreateView =>
          val query = viewPlan.query
          val child = viewPlan.child
          val tableSpec = TableSpec(properties = Map.empty[String, String],
            provider = Some("delta"), options = Map.empty[String, String],
            location = None, serde = None, comment = None, external = false)
          CreateTableAsSelect(
            name = child,
            query = query,
            partitioning = Seq.empty[Transform],
            tableSpec = tableSpec,
            writeOptions = Map.empty[String,String],
            ignoreIfExists = false
          )

        case nonDefaultCatalogCreateViewCommand: NonDefaultCatalogCreateViewCommand =>
          nonDefaultCatalogCreateViewCommand
        case _ => plan
      }
      substitutedPlan

    }else{
      plan
    }
  }


}

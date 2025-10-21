package org.apache.spark.sql.hive.plan.listener

import org.apache.spark.sql.catalyst.plans.logical.{AppendData, DeltaMergeInto, LogicalPlan, View}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, UpdateCommand}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.QueryExecutionListener

class CatalogQueryExecutionListener extends QueryExecutionListener{

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {


  //  println("Plan at listener end: ", qe.logical.prettyJson)
    val printableResult = ListenerUtil.getSQLTextIfExists(qe.analyzed)
    printableResult match {
      case Some(desc) =>
        println("Leaf nodes are "+ ListenerUtil.getCatables(qe.analyzed).mkString(","))
        println(String.format("%s...%s", "Hi final result is", desc))
      case None => println("Nothing in SQL")
    }

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println("failed")
  }

}



object ListenerUtil{



  def getSQLTextIfExists(plan:LogicalPlan):Option[String] = {
    plan.prettyJson
    val res = plan.find(pl => {
      pl.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined ||
        pl.expressions.exists(attr => attr.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined)
    })
    if(res.isDefined){
      val res1 = res.get.getTagValue(TreeNodeTag[String]("spark-sql")).getOrElse({
        val sqlsInReference = res.get.expressions.map(attr => attr.getTagValue(TreeNodeTag[String]("spark-sql"))).filter(_.isDefined)
        val exprResult = sqlsInReference.head match {
          case Some(sql) => sql
          case None => "None"
        }
        exprResult
      })
      Some(res1)
    }else{
      None
    }
  }

  def copyPlanTagsIfExists(source:LogicalPlan, target:LogicalPlan):Unit={
    getSQLTextIfExists(source) match {
      case Some(sql) => setSQLText(target, sql)
      case None => println("Nothing found")
    }
  }


  def getCatables(plan: LogicalPlan):Seq[String]={
    plan match {
      case ap: AppendData => Seq(ap.table.name)++getCatables(ap.query)
      case _ => plan.collectLeaves().toSeq.map {
        case d: DataSourceV2Relation => d.table.name()

        case up: UpdateCommand =>
          up.catalogTable match {
            case Some(ct) => ct.qualifiedName
            case None => up.toString()
          }
        case m: MergeIntoCommand => m.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => m.toString()
        }

        case dm: DeltaMergeInto => getCatables(dm.target).head

        case d: DeleteCommand => d.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => d.toString()
        }
        case l: LogicalRelation => l.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => l.toString()
        }

        case view: View => view.desc.qualifiedName
        case p => p.toString()
      }
    }
  }
  def setSQLText(plan: LogicalPlan, sql:String):Unit={
    println("Hello, Setting SQL "+ sql + " for "+ plan.toString())
    //plan.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
    plan.foreach(p=>{
      p.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
      p.expressions.foreach(attr => attr.setTagValue(TreeNodeTag[String]("spark-sql"), sql))
    })
  }
}

package org.apache.spark.sql.hive.plan.listener

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class CatalogQueryExecutionListener extends QueryExecutionListener{

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {


  //  println("Plan at listener end: ", qe.logical.prettyJson)
    val printableResult = ListenerUtil.getSQLTextIfExists(qe.analyzed)
    printableResult match {
      case Some(desc) => println(String.format("%s...%s", "Hi final result is", desc))
      case None => println("Nothing in SQL")
    }

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println("failed")
  }

}



object ListenerUtil{



  def getSQLTextIfExists(plan:LogicalPlan):Option[String] = {
    val res = plan.find(pl => {
      pl.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined ||
        pl.producedAttributes.filter(attr => attr.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined).nonEmpty
    })
    if(res.isDefined){
      val res1 = res.get.getTagValue(TreeNodeTag[String]("spark-sql")).getOrElse({
        val sqlsInReference = res.get.producedAttributes.map(attr => attr.getTagValue(TreeNodeTag[String]("spark-sql"))).filter(_.isDefined)
        sqlsInReference.head match {
          case Some(sql) => sql
          case None => plan.prettyJson
        }
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
  def setSQLText(plan: LogicalPlan, sql:String):Unit={
    println("Hello, Setting SQL "+ sql + " for "+ plan.toString())
    //plan.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
    plan.foreach(p=>{
      p.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
      p.producedAttributes.foreach(attr => attr.setTagValue(TreeNodeTag[String]("spark-sql"), sql))
    })
  }
}

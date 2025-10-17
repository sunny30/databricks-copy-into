package org.apache.spark.sql.hive.plan.listener

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.util.QueryExecutionListener

class CatalogQueryExecutionListener extends QueryExecutionListener{

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

    val res = qe.logical.find(pl => pl.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined)
    if(res.isDefined){
      val res1 = res.get.getTagValue(TreeNodeTag[String]("spark-sql")).get
      println("Hi there is: "+res1)
    }else{
      println("Hi there is: No SQL")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ???

}

//case class SparkSQLDummyListenerNode(child: LogicalPlan) extends UnaryNode{
//  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
//
//
//  override def output: Seq[Attribute] = child.output
//}

//object StripOriginalSqlTagRule extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    case t: SparkSQLDummyListenerNode => t.child: LogicalPlan
//  }
//}

object ListenerUtil{

//  def getSparkSQLPlan(plan: LogicalPlan):LogicalPlan = {
//    plan match {
//      case l:LeafRunnableCommand => l
//      case _ => SparkSQLDummyListenerNode(plan)
//    }
//  }

  def getSQLTextIfExists(plan:LogicalPlan):Option[String] = {
    val res = plan.find(pl => pl.getTagValue(TreeNodeTag[String]("spark-sql")).isDefined)
    if(res.isDefined){
      val res1 = res.get.getTagValue(TreeNodeTag[String]("spark-sql")).get
      Some(res1)
    }else{
      Some("No Result as tag")
    }
  }
  def setSQLText(plan: LogicalPlan, sql:String):Unit={
    println("Hello, Setting SQL "+ sql + " for "+ plan.toString())
    //plan.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
    plan.foreach(p=>p.setTagValue(TreeNodeTag[String]("spark-sql"), sql))

    println("Hello, Setting SQL "+ sql + " for "+ plan.toString())
    println("Setter "+ plan.getTagValue(TreeNodeTag[String]("spark-sql")).getOrElse("No value"))
  }
}

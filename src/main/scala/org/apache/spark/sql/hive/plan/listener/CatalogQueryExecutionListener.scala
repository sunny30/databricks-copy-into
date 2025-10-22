package org.apache.spark.sql.hive.plan.listener

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, DeltaMergeInto, LogicalPlan, ReplaceTableAsSelect, View}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, UpdateCommand}
import org.apache.spark.sql.execution.{ExplainUtils, QueryExecution}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.zookeeper.Op.Create

class CatalogQueryExecutionListener extends QueryExecutionListener{

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {



    val printableResult = ListenerUtil.getSQLTextIfExists(qe.analyzed)
    printableResult match {
      case Some(desc) =>
        println("Leaf nodes are "+ ListenerUtil.getCatables(qe.analyzed).mkString(","))
        println(String.format("%s...%s", "Hi final result is", desc))
      case None  =>
        println("Inside none in listener")
        println("Leaf nodes are "+ ListenerUtil.getCatables(qe.analyzed).mkString(","))
        var explainPlan = new PlanStringConcat()
        QueryPlan.append(qe.analyzed, explainPlan.append, verbose = false, addSuffix = true)
        println(String.format("%s...%s", "Hi final DataFrame result", explainPlan.toString()))
    }

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println("failed")
  }

}

object CrossThreadSqlHolder {

  private val SQLLocalPropertyKey = "user.sql.text"

  def setSqlText(sql: String): Unit = SparkSession.active.sparkContext.setLocalProperty(SQLLocalPropertyKey,sql)

  def getSqlText: String = SparkSession.active.sparkContext.getLocalProperty(SQLLocalPropertyKey)

  def clear(): Unit = SparkSession.active.sparkContext.setLocalProperty(SQLLocalPropertyKey, null)
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
      if(CrossThreadSqlHolder.getSqlText !=null) {
        val fallbackResponse = Some(CrossThreadSqlHolder.getSqlText)
        CrossThreadSqlHolder.clear()
        fallbackResponse
      }else{
        None
      }
    }
  }

  def copyPlanTagsIfExists(source:LogicalPlan, target:LogicalPlan):Unit={
    getSQLTextIfExists(source) match {
      case Some(sql) => setSQLText(target, sql)
      case None => println("Nothing found")
    }
  }


  def getCatables(plan: LogicalPlan):Seq[String]={
    val leafNodes = plan match {
      case ap: AppendData => Seq(ap.table.name)++getCatables(ap.query)
      case ctas@ CreateTableAsSelect(r@ResolvedIdentifier(catalog, ident),_,query,_,_,_,_) => Seq(catalog.name()+"."+ident.name()) ++ getCatables(query)
      case rtas@ReplaceTableAsSelect(r@ResolvedIdentifier(catalog, ident), _, query, _, _, _, _) => Seq(catalog.name()+"."+ident.name()) ++ getCatables(query)
      case _ => plan.collectLeaves().toSeq.map {
        case d: DataSourceV2Relation => d.table.name()
        case ds: DataSourceV2ScanRelation => getCatables(ds.relation).head

        case up: UpdateCommand =>
          up.catalogTable match {
            case Some(ct) => ct.qualifiedName
            case None => String.format("%s.%s", "--NO REL",up.toString())
          }
        case m: MergeIntoCommand => m.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => String.format("%s.%s", "--NO REL",m.toString())
        }

        case dm: DeltaMergeInto => getCatables(dm.target).head

        case d: DeleteCommand => d.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => String.format("%s.%s", "--NO REL",d.toString())
        }
        case l: LogicalRelation => l.catalogTable match {
          case Some(ct) => ct.qualifiedName
          case None => String.format("%s.%s", "--NO REL",l.toString())
        }

        case view: View => view.desc.qualifiedName
        case p => String.format("%s.%s", "--NO REL",p.toString())
      }
    }

    leafNodes

  }
  def setSQLText(plan: LogicalPlan, sql:String):Unit={
    println("Hello, Setting SQL "+ sql + " for "+ plan.toString())
    CrossThreadSqlHolder.setSqlText(sql)
    //plan.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
    plan.foreach(p=>{
      p.setTagValue(TreeNodeTag[String]("spark-sql"), sql)
      p.expressions.foreach(attr => attr.setTagValue(TreeNodeTag[String]("spark-sql"), sql))
    })
  }
}

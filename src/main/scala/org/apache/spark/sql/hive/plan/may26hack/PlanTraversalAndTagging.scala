package org.apache.spark.sql.hive.plan.may26hack

import io.prometheus.client.Counter.Child
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Filter, GlobalLimit, Join, LeafNode, LocalLimit, LogicalPlan, Project, UnaryNode, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PlanTraversalAndTagging(spark:SparkSession) {


  def abstractTraverse(plan: LogicalPlan):Unit={
    plan match {

      case u:UnaryNode =>
        abstractTraverse(u.child)
        if(isPlansBelongToSameCut(Seq(u.child))){
          val catalogName = gatherCatalog(u.child).get
          tagPlanWithExternalCatalog(u, catalogName,true)
        }
      case b:BinaryNode =>
        abstractTraverse(b.left)
        abstractTraverse(b.right)
        if(isPlansBelongToSameCut(Seq(b.left, b.right))){
          copyTagsFromChild(b.left, b)
        }
      case union:Union =>
        union.children.foreach(abstractTraverse)
        if (isPlansBelongToSameCut(union.children)) {
          copyTagsFromChild(union.children(0), union)
        }
      case l: LeafNode =>
        tagxternalCatalogRelationIfExists(l)

        //put the logic of external catalog test
      case _ => println("no need to traverse")
    }

  }


  def tagxternalCatalogRelationIfExists(plan: LogicalPlan):Unit = {

    plan match {
      case dataSourceV2Relation@DataSourceV2Relation(table: V2Table, output: Seq[AttributeReference], catalog: Option[CatalogPlugin], _, options: CaseInsensitiveStringMap)=>
        if(isExternalCatalogPlugin(catalog)){
          val catalogName = catalog.get.name()
          tagPlanWithExternalCatalog(dataSourceV2Relation,catalogName,true)
        }

      case lr@LogicalRelation(relation, output, Some(catalogTable), isStreaming)  if(isExternalCatalog(catalogTable.identifier.catalog.get)) =>
        tagPlanWithExternalCatalog(lr,catalogTable.identifier.catalog.get,true)

      case _ => println("no need to tag")
    }
  }

  def copyTagsFromChild(child: LogicalPlan, parent: LogicalPlan): Unit = {
    // tags is Map[TreeNodeTag[_], Any] on TreeNode
    parent.copyTagsFrom(child)
  }


  def isExternalCatalog(catalogName:String):Boolean = {
      catalogName.equalsIgnoreCase("ecat")
  }

  def isExternalCatalogPlugin(catalog: Option[CatalogPlugin]): Boolean = {
    catalog.isDefined && isExternalCatalog(catalog.get.name())
  }

  def tagPlanWithExternalCatalog(plan: LogicalPlan, catalogName: String, isExternal:Boolean):Unit = {
    val isExternalCatalogtagKey = "is_external_catalog"
    val catalogIdKey = "catalog.id"
    plan.setTagValue(TreeNodeTag[String](isExternalCatalogtagKey), isExternal.toString)
    plan.setTagValue(TreeNodeTag[String](catalogIdKey), catalogName)

  }



  def isPlanContainExternalCatalogTag(plan: LogicalPlan):Boolean={
    plan.getTagValue(TreeNodeTag[String]("is_external_catalog")).isDefined &&
    plan.getTagValue(TreeNodeTag[String]("catalog.id")).isDefined
  }

  def gatherCatalog(plan: LogicalPlan): Option[String] = {
    plan.getTagValue(TreeNodeTag[String]("catalog.id"))
  }

  def gatherExternalAndCatalogStatus(plan: LogicalPlan):(Boolean, Option[String]) = {
    (isPlanContainExternalCatalogTag(plan), gatherCatalog(plan))
  }

  def isPlansBelongToSameCut(plans:Seq[LogicalPlan]):Boolean = {
    val res = plans.map( p => gatherExternalAndCatalogStatus(p))
    val externalStatus = res.map(_._1)
    val catalogsInfo = res.map(_._2)
    externalStatus.distinct.length == 1 &&
      externalStatus.distinct.head &&
      catalogsInfo.distinct.length == 1 &&
      catalogsInfo.distinct.head.isDefined

  }

  def getDSOptionMap(plan: LogicalPlan, sql: String): Map[String, String] = {
    if (isPlanContainExternalCatalogTag(plan)) {

      Map(
        "catalog.id" -> gatherCatalog(plan).get,
        "pushdown.sql" -> sql
      )
    } else {
      Map.empty[String, String]
    }
  }

}

class ExternalCatalogCutAnalyzer(session: SparkSession)
  extends Rule[LogicalPlan] with Logging {

  def isPlanAlreadyTraversedAndTag(plan: LogicalPlan): Boolean = {
    plan.getTagValue(TreeNodeTag[String]("cut-included")).isDefined
  }
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val pt = new PlanTraversalAndTagging(session)
    pt.abstractTraverse(plan)
    plan transformDown{
//stop to recur into infinite recursion
      case p:LogicalPlan if(isPlanAlreadyTraversedAndTag(p)) =>
        p

      case mp:UnaryNode if ((mp.isInstanceOf[Project] || mp.isInstanceOf[Filter]) && pt.isPlanContainExternalCatalogTag(mp)) =>
        val prj = Project(Seq(UnresolvedStar(None)), mp)
        val sql = SparkPlanToSQL.toSQL(prj)



        val ds  = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = Some(mp.child.schema),
          partitionColumns = Seq.empty[String],
          bucketSpec = None,
          className = "hubquery",
          options = pt.getDSOptionMap(plan, sql),
          catalogTable = None)
         // pl
       val lr = LogicalRelation(ds.resolveRelation(false), false)

        lr.copy(output = mp.output.map(_.asInstanceOf[AttributeReference]))
        //lr.setTagValue(TreeNodeTag[String]("cut-defined"), catalogName)

        //pl
      case join: Join =>
        val prj = Project(Seq(UnresolvedStar(None)), join)
        val sql = SparkPlanToSQL.toSQL(prj)
        val ds = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = Some(join.schema),
          partitionColumns = Seq.empty[String],
          bucketSpec = None,
          className = "hubquery",
          options = pt.getDSOptionMap(plan, sql),
          catalogTable = None)
        val lr = LogicalRelation(ds.resolveRelation(false), false)

        lr.copy(output = join.output.map(_.asInstanceOf[AttributeReference]))



      case union: Union =>
        val prj = Project(Seq(UnresolvedStar(None)), union)
        val sql = SparkPlanToSQL.toSQL(prj)
        val ds = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = Some(union.schema),
          partitionColumns = Seq.empty[String],
          bucketSpec = None,
          className = "hubquery",
          options = pt.getDSOptionMap(plan, sql),
          catalogTable = None)
        val lr = LogicalRelation(ds.resolveRelation(false), false)

        lr.copy(output = union.output.map(_.asInstanceOf[AttributeReference]))

      case p:LogicalPlan => p

    }
  }


}

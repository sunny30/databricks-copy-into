package org.apache.spark.sql.hive.plan.may26hack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode, Union}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.connector.V2Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PlanTraversalAndTagging(spark:SparkSession) {


  def abstractTraverse(plan: LogicalPlan):Unit={
    plan match {
      case u:UnaryNode =>
        abstractTraverse(u.child)
        if(isPlansBelongToSameCut(Seq(u.child))){
          tagxternalCatalogRelationIfExists(u)
        }
      case b:BinaryNode =>
        abstractTraverse(b.left)
        abstractTraverse(b.right)
        if(isPlansBelongToSameCut(Seq(b.left, b.right))){
          tagxternalCatalogRelationIfExists(b)
        }
      case union:Union =>
        union.children.foreach(abstractTraverse)
        if (isPlansBelongToSameCut(union.children)) {
          tagxternalCatalogRelationIfExists(union)
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
          tagPlanWithExternalCatalog(plan,catalogName,true)
        }

      case _ => println("no need to tag")
    }
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

}

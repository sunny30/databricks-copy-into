package org.apache.spark.sql.hive.plan.may26hack

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class PlanTraversalAndTagging(spark:SparkSession) {


  def abstractTraverse(plan: LogicalPlan):Unit={
    plan match {
      case u:UnaryNode =>
        abstractTraverse(u.child)
      case b:BinaryNode =>
        abstractTraverse(b.left)
        abstractTraverse(b.right)
      case union:Union =>
        union.children.foreach(abstractTraverse)

      case l: LeafNode =>
        //put the logic of external catalog test
    }

  }

}

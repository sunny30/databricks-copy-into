package org.apache.spark.sql.hive.plan.spark.sql.execution

import org.apache.iceberg.spark.Spark3Util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceBranch
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceTag
import org.apache.spark.sql.catalyst.plans.logical.DropBranch
import org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField
import org.apache.spark.sql.catalyst.plans.logical.DropTag
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField
import org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AddPartitionFieldExec, CreateOrReplaceBranchExec, CreateOrReplaceTagExec, DropBranchExec, DropIdentifierFieldsExec, DropPartitionFieldExec, DropTagExec, ReplacePartitionFieldExec, SetIdentifierFieldsExec, SetWriteDistributionAndOrderingExec}

import scala.jdk.CollectionConverters._

case class IcebergStrategy(spark: SparkSession) extends Strategy with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {


    case AddPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform, name) =>
      AddPartitionFieldExec(catalog, ident, transform, name) :: Nil

    case CreateOrReplaceBranch(
    IcebergCatalogAndIdentifier(catalog, ident), branch, branchOptions, create, replace, ifNotExists) =>
      CreateOrReplaceBranchExec(catalog, ident, branch, branchOptions, create, replace, ifNotExists) :: Nil

    case CreateOrReplaceTag(
    IcebergCatalogAndIdentifier(catalog, ident), tag, tagOptions, create, replace, ifNotExists) =>
      CreateOrReplaceTagExec(catalog, ident, tag, tagOptions, create, replace, ifNotExists) :: Nil

    case DropBranch(IcebergCatalogAndIdentifier(catalog, ident), branch, ifExists) =>
      DropBranchExec(catalog, ident, branch, ifExists) :: Nil

    case DropTag(IcebergCatalogAndIdentifier(catalog, ident), tag, ifExists) =>
      DropTagExec(catalog, ident, tag, ifExists) :: Nil

    case DropPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform) =>
      DropPartitionFieldExec(catalog, ident, transform) :: Nil

    case ReplacePartitionField(IcebergCatalogAndIdentifier(catalog, ident), transformFrom, transformTo, name) =>
      ReplacePartitionFieldExec(catalog, ident, transformFrom, transformTo, name) :: Nil

    case SetIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      SetIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case DropIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      DropIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case SetWriteDistributionAndOrdering(
    IcebergCatalogAndIdentifier(catalog, ident), distributionMode, ordering) =>
      SetWriteDistributionAndOrderingExec(catalog, ident, distributionMode, ordering) :: Nil

      case _ => Nil
  }



  private object IcebergCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: TableCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))

        case _ =>
          None
      }
    }
  }
}

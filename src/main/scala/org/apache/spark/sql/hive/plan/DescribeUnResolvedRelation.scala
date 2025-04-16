package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedRelation, UnresolvedTable, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.plans.logical.{DescribeRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}


class DescribeUnResolvedRelation(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

      case d@DescribeRelation(u@UnresolvedTableOrView(multipartIdentifier, _, _), partitionSpec, isExtended, output) =>
        val res = if (multipartIdentifier.size == 3) {
          (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
        } else if (multipartIdentifier.size == 2) {
          ("spark_catalog", multipartIdentifier(0), multipartIdentifier(1))
        } else {
          ("spark_catalog", "default", multipartIdentifier(0))
        }
        val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(res._1).asTableCatalog
        val tid = Identifier.of(Seq(res._2).toArray, res._3)
        val tc = sessionCatalog.loadTable(tid)
        val viewCt = sessionCatalog.loadTable(tid, null)
        if(tc == null && viewCt !=null){
          val rt = ResolvedTable.create(sessionCatalog, tid, viewCt)
          d.copy(relation = rt)
        }else{
          d
        }


      case u: UnresolvedTable =>
        println("Inside UnresolvedTable for DescribeUnresolved")
        if (u.multipartIdentifier.size == 3) {
          val catName = u.multipartIdentifier(0)
          val dbName = u.multipartIdentifier(1)
          val tableName = u.multipartIdentifier(2)
          val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
          val tid = Identifier.of(Seq(dbName).toArray, tableName)
          val tc = sessionCatalog.loadTable(tid)
          if (tc == null) {
            val viewCt = sessionCatalog.loadTable(tid, null)
            if (viewCt != null) {
              ResolvedTable.create(sessionCatalog, tid, viewCt)
            } else {
              u
            }

          } else {
            tc match {
              case d: DeltaTableV2 => (ResolvedTable.create(sessionCatalog, u.multipartIdentifier.asIdentifier, d))
              case _ => u
            }
          }
        } else {
          u
        }

      case uv: UnresolvedTableOrView =>
        println("Inside UnresolvedTable for DescribeUnresolved")
        if (uv.multipartIdentifier.size == 3) {
          val catName = uv.multipartIdentifier(0)
          val dbName = uv.multipartIdentifier(1)
          val tableName = uv.multipartIdentifier(2)
          val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
          val tid = Identifier.of(Seq(dbName).toArray, tableName)
          val tc = sessionCatalog.loadTable(tid)
          if (tc == null) {
            val viewCt = sessionCatalog.loadTable(tid, null)
            if (viewCt != null) {
              ResolvedTable.create(sessionCatalog, tid, viewCt)
            } else {
              uv
            }

          } else {
            tc match {
              case d: DeltaTableV2 => (ResolvedTable.create(sessionCatalog, uv.multipartIdentifier.asIdentifier, d))
              case _ => uv
            }
          }
        } else {
          uv
        }



      case pl:LogicalPlan => pl

    }




}

package org.apache.spark.sql.hive.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteFromTable, DeltaMergeInto, InsertAction, InsertStarAction, LogicalPlan, MergeIntoTable, SubqueryAlias, UpdateAction, UpdateStarAction, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.delta.DeltaRelation.recordFrameProfile
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ShowCatalogViews
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

class ResolveDeltaCrudOperation(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging{

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp  {


    case d: DeleteFromTable =>
      val newQuery = CLSUtils.removeSecureProjection(d.table)
      d.copy(table = newQuery)

    case m: MergeIntoTable =>
      val merge = m.copy(
        targetTable = CLSUtils.getSecureRelation(m.targetTable),
        sourceTable = CLSUtils.getSecureRelation(m.sourceTable))
      if (merge.targetTable.resolved && merge.sourceTable.resolved) {
        expandTargetOnlyMergeStarActions(merge)
      } else {
        merge
      }

    case m: DeltaMergeInto =>
      m.copy(
        target = CLSUtils.removeSecureProjection(m.target),
        source = CLSUtils.removeSecureProjection(m.source))

    case u:UpdateTable  =>
      val newQuery = CLSUtils.removeSecureProjection(u.table)
      u.copy(table = newQuery)


    case dsv2@DataSourceV2Relation(d: DeltaTableV2, _, _, _, options) if (d.timeTravelOpt.isDefined) =>
      fromV2Relation(d,dsv2, options)



//    case f@FakeLogicalPlan(exprs: Seq[Expression],
//    children: Seq[LogicalPlan]) =>
//      val newChildren = children.map(f => f match {
//        case s: SubqueryAlias => CLSUtils.removeSecureProjection(s)
//        case pl:LogicalPlan => pl
//      })
//      f.copy(children = newChildren)


    case pl: LogicalPlan => pl
  }


  def fromV2Relation(
                      d: DeltaTableV2,
                      v2Relation: DataSourceV2Relation,
                      options: CaseInsensitiveStringMap): LogicalRelation = {

    var isCDC = false
    val relation = d.withOptions(options.asScala.toMap).toBaseRelation
    val output = if (CDCReader.isCDCRead(options)) {
      // Handles cdc for the spark.read.options().table() code path
      toAttributes(relation.schema)
    } else {
      v2Relation.output
    }
    val lr = LogicalRelation(relation, output, d.ttSafeCatalogTable, isStreaming = false)
    lr.setTagValue(TreeNodeTag[String]("delta-time-travel-read"), "true")
    lr
  }


  private def expandTargetOnlyMergeStarActions(merge: MergeIntoTable): MergeIntoTable = {
    val sourceOutput = merge.sourceTable.output
    val resolver = session.sessionState.conf.resolver

    def sourceAttrFor(targetAttr: Attribute) =
      sourceOutput.find(sourceAttr => resolver(sourceAttr.name, targetAttr.name))

    val hasTargetOnlyColumns = merge.targetTable.output.exists(targetAttr => sourceAttrFor(targetAttr).isEmpty)
    val hasSourceOnlyColumns = sourceOutput.exists { sourceAttr =>
      !merge.targetTable.output.exists(targetAttr => resolver(targetAttr.name, sourceAttr.name))
    }
    val canEvolveSchema = session.sessionState.conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)

    if (!hasTargetOnlyColumns || (hasSourceOnlyColumns && canEvolveSchema)) {
      merge
    } else {
      val matchedActions = merge.matchedActions.map {
        case UpdateStarAction(condition) =>
          val assignments = merge.targetTable.output.map { targetAttr =>
            Assignment(targetAttr, sourceAttrFor(targetAttr).getOrElse(targetAttr))
          }
          UpdateAction(condition, assignments)
        case other => other
      }

      val notMatchedActions = merge.notMatchedActions.map {
        case InsertStarAction(condition) =>
          val assignments = merge.targetTable.output.flatMap { targetAttr =>
            sourceAttrFor(targetAttr).map(sourceAttr => Assignment(targetAttr, sourceAttr))
          }
          InsertAction(condition, assignments)
        case other => other
      }
      merge.copy(
        matchedActions = matchedActions,
        notMatchedActions = notMatchedActions)
    }
  }

}

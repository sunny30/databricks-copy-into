package org.apache.spark.sql.hive.plan.spark.sql.execution.plan
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.{PartitionSpec, ResolvedPartitionSpec, UnresolvedAttribute, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{And, Cast, EqualTo, Expression, IsNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan, SubqueryAlias, TruncatePartition, TruncateTable}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaRelation
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hive.plan.listener.ListenerUtil
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Shared helpers that rewrite `TRUNCATE TABLE` logical plans on Delta tables so Spark can fall back
 * to the V1 command implementation. Delta 3.2 advertises truncate support but lacks the V2
 * interfaces, so we translate each truncate into an equivalent delete.
 */
object DeltaTruncateUtils extends Logging {
  private val LogPrefix = "[centrify-truncate]"
  private val ResolvedTagValue = "resolved"

  /**
   * Redirects a `TRUNCATE TABLE` statement on a Delta table to the legacy V1 delete-all command.
   * Delta 3.2 advertises the truncate capability but does not implement the V2 interface, so Spark
   * otherwise surfaces `table does not support truncates`. By swapping in a V1 [[DeltaRelation]]
   * we reuse the battle-tested delete code path that physically clears the table.
   *
   * Example: `TRUNCATE TABLE cat1.default.orders_delta` becomes
   * `DELETE FROM cat1.default.orders_delta WHERE true`, which Delta V1 executes successfully.
   */
  def rewriteTruncate(
                       session: SparkSession,
                       resolverTag: TreeNodeTag[String],
                       command: TruncateTable): LogicalPlan = {
    if (command.getTagValue(resolverTag).isDefined) {
      logInfo(s"$LogPrefix Skipping rewrite for already-tagged TRUNCATE TABLE command.")
      command
    } else {
      rewriteDeltaChildForTruncate(command.table, resolverTag) match {
        case Some((deltaPlan, deltaTable)) =>
          logInfo(
            s"$LogPrefix Rewriting TRUNCATE TABLE for Delta table " +
              s"${deltaTable.v1Table.identifier.unquotedString} as DELETE (all rows).")
          val delete = DeleteFromTable(deltaPlan, Literal.TrueLiteral)
          ListenerUtil.copyPlanTagsIfExists(command, delete)
          delete.setTagValue(resolverTag, ResolvedTagValue)
          deltaPlan.foreach(p=>p.setTagValue(resolverTag, ResolvedTagValue))
          delete
        case None =>
          logInfo(s"$LogPrefix Leaving TRUNCATE TABLE unchanged (non-Delta or unresolved table).")
          command
      }
    }
  }

  /**
   * Same as [[rewriteTruncate]] but for `TRUNCATE TABLE ... PARTITION`. We translate the
   * user-supplied partition spec into a delete predicate so the V1 command can remove only the
   * targeted partitions.
   *
   * Example: `TRUNCATE TABLE cat1.default.orders_delta PARTITION (state = 'CA', country = 'US')`
   * becomes `DELETE FROM cat1.default.orders_delta WHERE state = 'CA' AND country = 'US'`.
   */
  def rewritePartitionTruncate(
                                session: SparkSession,
                                resolverTag: TreeNodeTag[String],
                                command: TruncatePartition): LogicalPlan = {
    if (command.getTagValue(resolverTag).isDefined) {
      logInfo(s"$LogPrefix Skipping rewrite for already-tagged TRUNCATE TABLE ... PARTITION command.")
      command
    } else {
      rewriteDeltaChildForTruncate(command.table, resolverTag) match {
        case Some((deltaPlan, deltaTable)) =>
          buildPartitionPredicate(session, command.partitionSpec, deltaTable.v1Table) match {
            case Some(predicate) =>
              logInfo(
                s"$LogPrefix Rewriting TRUNCATE TABLE PARTITION for Delta table " +
                  s"${deltaTable.v1Table.identifier.unquotedString} with predicate ${predicate.sql}.")
              val delete = DeleteFromTable(deltaPlan, predicate)
              ListenerUtil.copyPlanTagsIfExists(command, delete)
              delete.setTagValue(resolverTag, ResolvedTagValue)
              deltaPlan.foreach(p=>p.setTagValue(resolverTag, ResolvedTagValue))
              delete
            case None =>
              logInfo(
                s"$LogPrefix Unable to build partition predicate for Delta table " +
                  s"${deltaTable.v1Table.identifier.unquotedString}; leaving command unchanged.")
              command
          }
        case None =>
          logInfo(s"$LogPrefix Leaving TRUNCATE TABLE PARTITION unchanged (non-Delta or unresolved table).")
          command
      }
    }
  }

  /**
   * Ensures the child plan of a truncate command is backed by a V1 [[DeltaRelation]]. The analyzer
   * can surface a Delta table in several wrappers (resolved identifier, bare V2 relation, aliased
   * relation), so this helper peels away those layers, reconstructs the V1 relation, and returns it
   * alongside the original [[DeltaTableV2]] metadata. The caller then plugs the relation into a V1
   * delete command.
   */
  private def rewriteDeltaChildForTruncate(
                                            plan: LogicalPlan,
                                            resolverTag: TreeNodeTag[String]): Option[(LogicalPlan, DeltaTableV2)] = {
    plan match {
      case resolved: org.apache.spark.sql.catalyst.analysis.ResolvedTable =>
        resolved.table match {
          case delta: DeltaTableV2 =>
            val dsRelation = DataSourceV2Relation.create(
              table = delta,
              catalog = Some(resolved.catalog),
              identifier = Some(resolved.identifier))
            ListenerUtil.copyPlanTagsIfExists(resolved, dsRelation)
            val deltaRelation = DeltaRelation.fromV2Relation(delta, dsRelation, dsRelation.options)
            ListenerUtil.copyPlanTagsIfExists(dsRelation, deltaRelation)
            Some(deltaRelation -> delta)
          case _ =>
            None
        }
      case relation: DataSourceV2Relation if relation.table.isInstanceOf[DeltaTableV2] =>
        val deltaRelation = DeltaRelation.fromV2Relation(
          relation.table.asInstanceOf[DeltaTableV2],
          relation,
          relation.options)
        ListenerUtil.copyPlanTagsIfExists(relation, deltaRelation)
        Some(deltaRelation -> relation.table.asInstanceOf[DeltaTableV2])
      case alias: SubqueryAlias =>
        rewriteDeltaChildForTruncate(alias.child, resolverTag).map { case (childPlan, deltaTable) =>
          val updatedAlias = alias.withNewChildren(Seq(childPlan))
          ListenerUtil.copyPlanTagsIfExists(alias, updatedAlias)
          updatedAlias -> deltaTable
        }
      case _ =>
        None
    }
  }

  /**
   * Converts Spark's partition spec representation into the boolean predicate expected by
   * [[DeleteFromTable]]. The spec may arrive resolved (using internal rows) or unresolved
   * (string/option values from the parser), and may contain partial keys. We normalise the values
   * and stitch them together with `AND`.
   */
  private def buildPartitionPredicate(
                                       session: SparkSession,
                                       spec: PartitionSpec,
                                       catalogTable: CatalogTable): Option[Expression] = {
    spec match {
      case resolved: ResolvedPartitionSpec =>
        val resolver = session.sessionState.conf.resolver
        val partitionSchema = catalogTable.partitionSchema
        logInfo(
          s"$LogPrefix Building predicate from resolved partition spec " +
            s"${resolved.names.mkString(",")} for table ${catalogTable.identifier.unquotedString}.")
        val predicates = resolved.names.zipWithIndex.flatMap { case (name, idx) =>
          partitionSchema.find(field => resolver(field.name, name)).map { field =>
            val rawValue = resolved.ident.get(idx, field.dataType)
            val attribute = UnresolvedAttribute(field.name)
            if (rawValue == null) {
              IsNull(attribute)
            } else {
              val converter = CatalystTypeConverters.createToScalaConverter(field.dataType)
              val scalaValue = converter(rawValue)
              EqualTo(attribute, Literal.create(scalaValue, field.dataType))
            }
          }
        }
        predicates.reduceLeftOption(And)
      case unresolved: UnresolvedPartitionSpec =>
        logInfo(
          s"$LogPrefix Building predicate from unresolved partition spec ${unresolved.spec} " +
            s"for table ${catalogTable.identifier.unquotedString}.")
        Some(buildPredicateFromSpec(session, unresolved.spec, catalogTable))
      case _ =>
        None
    }
  }

  /**
   * Converts an unresolved partition spec (string map straight from the parser) into the canonical
   * boolean predicate that matches rows in the Delta table. The values are normalised via
   * [[unwrapPartitionValue]] and combined with `AND`.
   */
  private def buildPredicateFromSpec(
                                      session: SparkSession,
                                      spec: TablePartitionSpec,
                                      catalogTable: CatalogTable): Expression = {
    val resolver = session.sessionState.conf.resolver
    val partitionSchema = catalogTable.partitionSchema
    val predicates = spec.iterator.flatMap { case (key, rawValue) =>
      val attr = UnresolvedAttribute(key)
      unwrapPartitionValue(rawValue) match {
        case None =>
          Some(IsNull(attr))
        case Some(actual) =>
          buildEqualityPredicate(attr, key, actual, partitionSchema, resolver)
      }
    }.toSeq
    val combined = predicates.reduceOption[Expression](And).getOrElse(Literal.TrueLiteral)
    logInfo(s"$LogPrefix Combined partition predicate: ${combined.sql}")
    combined
  }

  /**
   * Spark stores partition specs as Maps whose values can be null literals or `Option[String]`.
   * This helper normalises the value into `Some(actual)` or `None` so the caller can decide whether
   * to build `=` or `IS NULL`.
   */
  private def unwrapPartitionValue(value: Any): Option[Any] = value match {
    case null => None
    case opt: Option[_] @unchecked => opt
    case other => Some(other)
  }

  /**
   * Given a partition column and a concrete value (already normalised), produce the corresponding
   * equality expression. When the column is known to the table we cast the literal to the column's
   * data type so the delete predicate honours Spark's coercion rules. If we cannot find a matching
   * partition column we still build the equality predicate so downstream resolution can raise the
   * appropriate error.
   */
  private def buildEqualityPredicate(
                                      attr: UnresolvedAttribute,
                                      key: String,
                                      value: Any,
                                      partitionSchema: StructType,
                                      resolver: (String, String) => Boolean): Option[Expression] = {
    partitionSchema.find(field => resolver(field.name, key)) match {
      case Some(field) =>
        val baseLiteral = Literal(value.toString)
        val typedLiteral =
          if (field.dataType == StringType) baseLiteral else Cast(baseLiteral, field.dataType)
        Some(EqualTo(attr, typedLiteral))
      case None =>
        Some(EqualTo(attr, Literal(value.toString)))
    }
  }
}
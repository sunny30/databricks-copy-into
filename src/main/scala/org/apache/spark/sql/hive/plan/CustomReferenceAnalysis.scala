package org.apache.spark.sql.hive.plan

import java.util
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success, Try}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, AnalysisErrorAt, ColumnResolutionHelper, EliminateSubqueryAliases, NamedRelation, ResolveColumnDefaultInInsert, ResolveReferencesInAggregate, ResolveReferencesInSort, ResolveReferencesInUpdate, Resolver, Star, UnresolvedAlias, UnresolvedAttribute, UnresolvedDeserializer, UnresolvedFunction, UnresolvedHaving, UnresolvedOrdinal, UnresolvedStar, withPosition}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{View => _, _}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.util.Success

object ResolveReferences extends Rule[LogicalPlan] with CustomColumnResolutionHelper {

  def resolver: Resolver = SQLConf.get.resolver

  /**
   * Return true if there're conflicting attributes among children's outputs of a plan
   *
   * The children logical plans may output columns with conflicting attribute IDs. This may happen
   * in cases such as self-join. We should wait for the rule [[DeduplicateRelations]] to eliminate
   * conflicting attribute IDs, otherwise we can't resolve columns correctly due to ambiguity.
   */
  def hasConflictingAttrs(p: LogicalPlan): Boolean = {
    p.children.length > 1 && {
      // Note that duplicated attributes are allowed within a single node,
      // e.g., df.select($"a", $"a"), so we should only check conflicting
      // attributes between nodes.
      val uniqueAttrs = mutable.HashSet[ExprId]()
      p.children.head.outputSet.foreach(a => uniqueAttrs.add(a.exprId))
      p.children.tail.exists { child =>
        val uniqueSize = uniqueAttrs.size
        val childSize = child.outputSet.size
        child.outputSet.foreach(a => uniqueAttrs.add(a.exprId))
        uniqueSize + childSize > uniqueAttrs.size
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    // Don't wait other rules to resolve the child plans of `InsertIntoStatement` as we need
    // to resolve column "DEFAULT" in the child plans so that they must be unresolved.
    case i: InsertIntoStatement => ResolveColumnDefaultInInsert(i)

    // Wait for other rules to resolve child plans first
    case p: LogicalPlan if !p.childrenResolved => p

    // Wait for the rule `DeduplicateRelations` to resolve conflicting attrs first.
    case p: LogicalPlan if hasConflictingAttrs(p) => p

    // If the projection list contains Stars, expand it.
    case p: Project if containsStar(p.projectList) =>
      p.copy(projectList = buildExpandedProjectList(p.projectList, p.child))
    // If the aggregate function argument contains Stars, expand it.
    case a: Aggregate if containsStar(a.aggregateExpressions) =>
      if (a.groupingExpressions.exists(_.isInstanceOf[UnresolvedOrdinal])) {
        throw QueryCompilationErrors.starNotAllowedWhenGroupByOrdinalPositionUsedError()
      } else {
        a.copy(aggregateExpressions = buildExpandedProjectList(a.aggregateExpressions, a.child))
      }
    case g: Generate if containsStar(g.generator.children) =>
      throw QueryCompilationErrors.invalidStarUsageError("explode/json_tuple/UDTF",
        extractStar(g.generator.children))
    // If the Unpivot ids or values contain Stars, expand them.
    case up: Unpivot if up.ids.exists(containsStar) ||
      // Only expand Stars in one-dimensional values
      up.values.exists(values => values.exists(_.length == 1) && values.exists(containsStar)) =>
      up.copy(
        ids = up.ids.map(buildExpandedProjectList(_, up.child)),
        // The inner exprs in Option[[exprs] is one-dimensional, e.g. Optional[[["*"]]].
        // The single NamedExpression turns into multiple, which we here have to turn into
        // Optional[[["col1"], ["col2"]]]
        values = up.values.map(_.flatMap(buildExpandedProjectList(_, up.child)).map(Seq(_)))
      )

    case u@Union(children, _, _)
      // if there are duplicate output columns, give them unique expr ids
      if children.exists(c => c.output.map(_.exprId).distinct.length < c.output.length) =>
      val newChildren = children.map { c =>
        if (c.output.map(_.exprId).distinct.length < c.output.length) {
          val existingExprIds = mutable.HashSet[ExprId]()
          val projectList = c.output.map { attr =>
            if (existingExprIds.contains(attr.exprId)) {
              // replace non-first duplicates with aliases and tag them
              val newMetadata = new MetadataBuilder().withMetadata(attr.metadata)
                .putNull("__is_duplicate").build()
              Alias(attr, attr.name)(explicitMetadata = Some(newMetadata))
            } else {
              // leave first duplicate alone
              existingExprIds.add(attr.exprId)
              attr
            }
          }
          Project(projectList, c)
        } else {
          c
        }
      }
      u.withNewChildren(newChildren)

    // A special case for Generate, because the output of Generate should not be resolved by
    // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
    case g@Generate(generator, _, _, _, _, _) if generator.resolved => g

    case g@Generate(generator, join, outer, qualifier, output, child) =>
      val newG = resolveExpressionByPlanOutput(generator, child, throws = true, allowOuter = true)
      if (newG.fastEquals(generator)) {
        g
      } else {
        Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
      }

    case mg: MapGroups if mg.dataOrder.exists(!_.resolved) =>
      // Resolve against `AppendColumns`'s children, instead of `AppendColumns`,
      // because `AppendColumns`'s serializer might produce conflict attribute
      // names leading to ambiguous references exception.
      val planForResolve = mg.child match {
        case appendColumns: AppendColumns => appendColumns.child
        case plan => plan
      }
      val resolvedOrder = mg.dataOrder
        .map(resolveExpressionByPlanOutput(_, planForResolve).asInstanceOf[SortOrder])
      mg.copy(dataOrder = resolvedOrder)

    // Left and right sort expression have to be resolved against the respective child plan only
    case cg: CoGroup if cg.leftOrder.exists(!_.resolved) || cg.rightOrder.exists(!_.resolved) =>
      // Resolve against `AppendColumns`'s children, instead of `AppendColumns`,
      // because `AppendColumns`'s serializer might produce conflict attribute
      // names leading to ambiguous references exception.
      val (leftPlanForResolve, rightPlanForResolve) = Seq(cg.left, cg.right).map {
        case appendColumns: AppendColumns => appendColumns.child
        case plan => plan
      } match {
        case Seq(left, right) => (left, right)
      }

      val resolvedLeftOrder = cg.leftOrder
        .map(resolveExpressionByPlanOutput(_, leftPlanForResolve).asInstanceOf[SortOrder])
      val resolvedRightOrder = cg.rightOrder
        .map(resolveExpressionByPlanOutput(_, rightPlanForResolve).asInstanceOf[SortOrder])

      cg.copy(leftOrder = resolvedLeftOrder, rightOrder = resolvedRightOrder)

    // Skips plan which contains deserializer expressions, as they should be resolved by another
    // rule: ResolveDeserializer.
    case plan if containsDeserializer(plan.expressions) => plan

    case a: Aggregate => ResolveReferencesInAggregate(a)

    // Special case for Project as it supports lateral column alias.
    case p: Project =>
      val resolvedNoOuter = p.projectList
        .map(resolveExpressionByPlanChildren(_, p, allowOuter = false))
      // Lateral column alias has higher priority than outer reference.
      val resolvedWithLCA = resolveLateralColumnAlias(resolvedNoOuter)
      val resolvedWithOuter = resolvedWithLCA.map(resolveOuterRef)
      p.copy(projectList = resolvedWithOuter.map(_.asInstanceOf[NamedExpression]))

    case o: OverwriteByExpression if o.table.resolved =>
      // The delete condition of `OverwriteByExpression` will be passed to the table
      // implementation and should be resolved based on the table schema.
      o.copy(deleteExpr = resolveExpressionByPlanOutput(o.deleteExpr, o.table))

    case u: UpdateTable => ResolveReferencesInUpdate(u)

    case m@MergeIntoTable(targetTable, sourceTable, _, _, _, _)
      if !m.resolved && targetTable.resolved && sourceTable.resolved =>

      EliminateSubqueryAliases(targetTable) match {
        case r: NamedRelation if r.skipSchemaResolution =>
          // Do not resolve the expression if the target table accepts any schema.
          // This allows data sources to customize their own resolution logic using
          // custom resolution rules.
          m

        case _ =>
          val newMatchedActions = m.matchedActions.map {
            case DeleteAction(deleteCondition) =>
              val resolvedDeleteCondition = deleteCondition.map(
                resolveExpressionByPlanChildren(_, m))
              DeleteAction(resolvedDeleteCondition)
            case UpdateAction(updateCondition, assignments) =>
              val resolvedUpdateCondition = updateCondition.map(
                resolveExpressionByPlanChildren(_, m))
              UpdateAction(
                resolvedUpdateCondition,
                // The update value can access columns from both target and source tables.
                resolveAssignments(assignments, m, MergeResolvePolicy.BOTH))
            case UpdateStarAction(updateCondition) =>
              val assignments = targetTable.output.map { attr =>
                Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
              }
              UpdateAction(
                updateCondition.map(resolveExpressionByPlanChildren(_, m)),
                // For UPDATE *, the value must be from source table.
                resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
            case o => o
          }
          val newNotMatchedActions = m.notMatchedActions.map {
            case InsertAction(insertCondition, assignments) =>
              // The insert action is used when not matched, so its condition and value can only
              // access columns from the source table.
              val resolvedInsertCondition = insertCondition.map(
                resolveExpressionByPlanOutput(_, m.sourceTable))
              InsertAction(
                resolvedInsertCondition,
                resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
            case InsertStarAction(insertCondition) =>
              // The insert action is used when not matched, so its condition and value can only
              // access columns from the source table.
              val resolvedInsertCondition = insertCondition.map(
                resolveExpressionByPlanOutput(_, m.sourceTable))
              val assignments = targetTable.output.map { attr =>
                Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
              }
              InsertAction(
                resolvedInsertCondition,
                resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
            case o => o
          }
          val newNotMatchedBySourceActions = m.notMatchedBySourceActions.map {
            case DeleteAction(deleteCondition) =>
              val resolvedDeleteCondition = deleteCondition.map(
                resolveExpressionByPlanOutput(_, targetTable))
              DeleteAction(resolvedDeleteCondition)
            case UpdateAction(updateCondition, assignments) =>
              val resolvedUpdateCondition = updateCondition.map(
                resolveExpressionByPlanOutput(_, targetTable))
              UpdateAction(
                resolvedUpdateCondition,
                // The update value can access columns from the target table only.
                resolveAssignments(assignments, m, MergeResolvePolicy.TARGET))
            case o => o
          }

          val resolvedMergeCondition = resolveExpressionByPlanChildren(m.mergeCondition, m)
          m.copy(mergeCondition = resolvedMergeCondition,
            matchedActions = newMatchedActions,
            notMatchedActions = newNotMatchedActions,
            notMatchedBySourceActions = newNotMatchedBySourceActions)
      }

    // UnresolvedHaving can host grouping expressions and aggregate functions. We should resolve
    // columns with `agg.output` and the rule `ResolveAggregateFunctions` will push them down to
    // Aggregate later.
    case u@UnresolvedHaving(cond, agg: Aggregate) if !cond.resolved =>
      u.mapExpressions { e =>
        // Columns in HAVING should be resolved with `agg.child.output` first, to follow the SQL
        // standard. See more details in SPARK-31519.
        val resolvedWithAgg = resolveColWithAgg(e, agg)
        resolveExpressionByPlanChildren(resolvedWithAgg, u, allowOuter = true)
      }

    // RepartitionByExpression can host missing attributes that are from a descendant node.
    // For example, `spark.table("t").select($"a").repartition($"b")`. We can resolve `b` with
    // table `t` even if there is a Project node between the table scan node and Sort node.
    // We also need to propagate the missing attributes from the descendant node to the current
    // node, and project them way at the end via an extra Project.
    case r@RepartitionByExpression(partitionExprs, child, _, _)
      if !r.resolved || r.missingInput.nonEmpty =>
      val resolvedNoOuter = partitionExprs.map(resolveExpressionByPlanChildren(_, r))
      val (newPartitionExprs, newChild) = resolveExprsAndAddMissingAttrs(resolvedNoOuter, child)
      // Outer reference has lower priority than this. See the doc of `ResolveReferences`.
      val finalPartitionExprs = newPartitionExprs.map(resolveOuterRef)
      if (child.output == newChild.output) {
        r.copy(finalPartitionExprs, newChild)
      } else {
        Project(child.output, r.copy(finalPartitionExprs, newChild))
      }

    // Filter can host both grouping expressions/aggregate functions and missing attributes.
    // The grouping expressions/aggregate functions resolution takes precedence over missing
    // attributes. See the classdoc of `ResolveReferences` for details.
    case f@Filter(cond, child) if !cond.resolved || f.missingInput.nonEmpty =>
      val resolvedNoOuter = resolveExpressionByPlanChildren(cond, f)
      val resolvedWithAgg = resolveColWithAgg(resolvedNoOuter, child)
      val (newCond, newChild) = resolveExprsAndAddMissingAttrs(Seq(resolvedWithAgg), child)
      // Outer reference has lowermost priority. See the doc of `ResolveReferences`.
      val finalCond = resolveOuterRef(newCond.head)
      if (child.output == newChild.output) {
        f.copy(condition = finalCond)
      } else {
        // Add missing attributes and then project them away.
        val newFilter = Filter(finalCond, newChild)
        Project(child.output, newFilter)
      }

    case s: Sort if !s.resolved || s.missingInput.nonEmpty => ResolveReferencesInSort(s)

    case q: LogicalPlan =>
      logTrace(s"Attempting to resolve ${q.simpleString(conf.maxToStringFields)}")
      q.mapExpressions(resolveExpressionByPlanChildren(_, q, allowOuter = true))
  }

  private object MergeResolvePolicy extends Enumeration {
    val BOTH, SOURCE, TARGET = Value
  }

  def resolveAssignments(
                          assignments: Seq[Assignment],
                          mergeInto: MergeIntoTable,
                          resolvePolicy: MergeResolvePolicy.Value): Seq[Assignment] = {
    assignments.map { assign =>
      val resolvedKey = assign.key match {
        case c if !c.resolved =>
          resolveMergeExprOrFail(c, Project(Nil, mergeInto.targetTable))
        case o => o
      }
      val resolvedValue = assign.value match {
        case c if !c.resolved =>
          val resolvePlan = resolvePolicy match {
            case MergeResolvePolicy.BOTH => mergeInto
            case MergeResolvePolicy.SOURCE => Project(Nil, mergeInto.sourceTable)
            case MergeResolvePolicy.TARGET => Project(Nil, mergeInto.targetTable)
          }
          val resolvedExpr = resolveExprInAssignment(c, resolvePlan)
          val withDefaultResolved = if (conf.enableDefaultColumns) {
            resolveColumnDefaultInAssignmentValue(
              resolvedKey,
              resolvedExpr,
              QueryCompilationErrors
                .defaultReferencesNotAllowedInComplexExpressionsInMergeInsertsOrUpdates())
          } else {
            resolvedExpr
          }
          checkResolvedMergeExpr(withDefaultResolved, resolvePlan)
          withDefaultResolved
        case o => o
      }
      Assignment(resolvedKey, resolvedValue)
    }
  }

  private def resolveMergeExprOrFail(e: Expression, p: LogicalPlan): Expression = {
    val resolved = resolveExprInAssignment(e, p)
    checkResolvedMergeExpr(resolved, p)
    resolved
  }

  private def checkResolvedMergeExpr(e: Expression, p: LogicalPlan): Unit = {
    e.references.filter(!_.resolved).foreach { a =>
      // Note: This will throw error only on unresolved attribute issues,
      // not other resolution errors like mismatched data types.
      val cols = p.inputSet.toSeq.map(attr => toSQLId(attr.name)).mkString(", ")
      a.failAnalysis(
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        messageParameters = Map(
          "objectName" -> toSQLId(a.name),
          "proposal" -> cols))
    }
  }

  private def containsDeserializer(exprs: Seq[Expression]): Boolean = {
    exprs.exists(_.exists(_.isInstanceOf[UnresolvedDeserializer]))
  }

  // Expand the star expression using the input plan first. If failed, try resolve
  // the star expression using the outer query plan and wrap the resolved attributes
  // in outer references. Otherwise throw the original exception.
  private def expand(s: Star, plan: LogicalPlan): Seq[NamedExpression] = {
    withPosition(s) {
      try {
        s.expand(plan, resolver)
      } catch {
        case e: AnalysisException =>
          AnalysisContext.get.outerPlan.map {
            // Only Project and Aggregate can host star expressions.
            case u@(_: Project | _: Aggregate) =>
              Try(s.expand(u.children.head, resolver)) match {
                case Success(expanded) => expanded.map(wrapOuterReference)
                case Failure(_) => throw e
              }
            // Do not use the outer plan to resolve the star expression
            // since the star usage is invalid.
            case _ => throw e
          }.getOrElse {
            throw e
          }
      }
    }
  }

  /**
   * Build a project list for Project/Aggregate and expand the star if possible
   */
  private def buildExpandedProjectList(
                                        exprs: Seq[NamedExpression],
                                        child: LogicalPlan): Seq[NamedExpression] = {
    exprs.flatMap {
      // Using Dataframe/Dataset API: testData2.groupBy($"a", $"b").agg($"*")
      case s: Star => expand(s, child)
      // Using SQL API without running ResolveAlias: SELECT * FROM testData2 group by a, b
      case UnresolvedAlias(s: Star, _) => expand(s, child)
      case o if containsStar(o :: Nil) => expandStarExpression(o, child) :: Nil
      case o => o :: Nil
    }.map(_.asInstanceOf[NamedExpression])
  }

  /**
   * Returns true if `exprs` contains a [[Star]].
   */
  def containsStar(exprs: Seq[Expression]): Boolean =
    exprs.exists(_.collect { case _: Star => true }.nonEmpty)

  private def extractStar(exprs: Seq[Expression]): Seq[Star] =
    exprs.flatMap(_.collect { case s: Star => s })

  /**
   * Expands the matching attribute.*'s in `child`'s output.
   */
  def expandStarExpression(expr: Expression, child: LogicalPlan): Expression = {
    expr.transformUp {
      case f0: UnresolvedFunction if !f0.isDistinct &&
        f0.nameParts.map(_.toLowerCase(Locale.ROOT)) == Seq("count") &&
        f0.arguments == Seq(UnresolvedStar(None)) =>
        // Transform COUNT(*) into COUNT(1).
        f0.copy(nameParts = Seq("count"), arguments = Seq(Literal(1)))
      case f1: UnresolvedFunction if containsStar(f1.arguments) =>
        // SPECIAL CASE: We want to block count(tblName.*) because in spark, count(tblName.*) will
        // be expanded while count(*) will be converted to count(1). They will produce different
        // results and confuse users if there is any null values. For count(t1.*, t2.*), it is
        // still allowed, since it's well-defined in spark.
        if (!conf.allowStarWithSingleTableIdentifierInCount &&
          f1.nameParts == Seq("count") &&
          f1.arguments.length == 1) {
          f1.arguments.foreach {
            case u: UnresolvedStar if u.isQualifiedByTable(child, resolver) =>
              throw QueryCompilationErrors
                .singleTableStarInCountNotAllowedError(u.target.get.mkString("."))
            case _ => // do nothing
          }
        }
        f1.copy(arguments = f1.arguments.flatMap {
          case s: Star => expand(s, child)
          case o => o :: Nil
        })
      case c: CreateNamedStruct if containsStar(c.valExprs) =>
        val newChildren = c.children.grouped(2).flatMap {
          case Seq(k, s: Star) => CreateStruct(expand(s, child)).children
          case kv => kv
        }
        c.copy(children = newChildren.toList)
      case c: CreateArray if containsStar(c.children) =>
        c.copy(children = c.children.flatMap {
          case s: Star => expand(s, child)
          case o => o :: Nil
        })
      case p: Murmur3Hash if containsStar(p.children) =>
        p.copy(children = p.children.flatMap {
          case s: Star => expand(s, child)
          case o => o :: Nil
        })
      case p: XxHash64 if containsStar(p.children) =>
        p.copy(children = p.children.flatMap {
          case s: Star => expand(s, child)
          case o => o :: Nil
        })
      // count(*) has been replaced by count(1)
      case o if containsStar(o.children) =>
        throw QueryCompilationErrors.invalidStarUsageError(s"expression `${o.prettyName}`",
          extractStar(o.children))
    }
  }
}

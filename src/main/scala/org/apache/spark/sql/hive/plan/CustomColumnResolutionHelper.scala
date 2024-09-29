package org.apache.spark.sql.hive.plan

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{ColumnResolutionHelper, GetColumnByOrdinal, GetViewColumnByNameAndOrdinal, TempResolvedColumn, UnresolvedAttribute, UnresolvedExtractValue, caseInsensitiveResolution, withPosition}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentDate, CurrentTimestamp, CurrentUser, Expression, ExtractValue, GroupingID, LambdaFunction, NamedExpression, VirtualColumn}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors

trait CustomColumnResolutionHelper extends  ColumnResolutionHelper {




  override def resolveExpressionByPlanChildren(
                                       e: Expression,
                                       q: LogicalPlan,
                                       allowOuter: Boolean = false): Expression = {
    val newE = if (e.exists(_.getTagValue(LogicalPlan.PLAN_ID_TAG).nonEmpty)) {
      // If the TreeNodeTag 'LogicalPlan.PLAN_ID_TAG' is attached, it means that the plan and
      // expression are from Spark Connect, and need to be resolved in this way:
      //    1, extract the attached plan id from the expression (UnresolvedAttribute only for now);
      //    2, top-down traverse the query plan to find the plan node that matches the plan id;
      //    3, if can not find the matching node, fail the analysis due to illegal references;
      //    4, resolve the expression with the matching node, if any error occurs here, apply the
      //    old code path;
      super.resolveExpressionByPlanChildren(e, q, allowOuter)
    } else {
      e
    }

    resolveExpression(
      newE,
      resolveColumnByName = nameParts => {
        q.resolveChildren(nameParts, conf.resolver)
      },
      getAttrCandidates = () => {
        assert(q.children.length == 1)
        q.children.head.output
      },
      throws = true,
      allowOuter = true)
  }


  private def resolveExpression(
                                 expr: Expression,
                                 resolveColumnByName: Seq[String] => Option[Expression],
                                 getAttrCandidates: () => Seq[Attribute],
                                 throws: Boolean,
                                 allowOuter: Boolean): Expression = {
    def innerResolve(e: Expression, isTopLevel: Boolean): Expression = withOrigin(e.origin) {
   //   if (e.resolved) return e
      val resolved = e match {
        case f: LambdaFunction if !f.bound => f

        case GetColumnByOrdinal(ordinal, _) =>
          val attrCandidates = getAttrCandidates()
          assert(ordinal >= 0 && ordinal < attrCandidates.length)
          attrCandidates(ordinal)

        case GetViewColumnByNameAndOrdinal(
        viewName, colName, ordinal, expectedNumCandidates, viewDDL) =>
          val attrCandidates = getAttrCandidates()
          val matched = attrCandidates.filter(a => conf.resolver(a.name, colName))
          if (matched.length != expectedNumCandidates) {
            throw QueryCompilationErrors.incompatibleViewSchemaChangeError(
              viewName, colName, expectedNumCandidates, matched, viewDDL)
          }
          matched(ordinal)

        case u@UnresolvedAttribute(nameParts) =>
          val result = withPosition(u) {
            resolveColumnByName(nameParts).orElse(resolveLiteralFunction(nameParts)).map {
              // We trim unnecessary alias here. Note that, we cannot trim the alias at top-level,
              // as we should resolve `UnresolvedAttribute` to a named expression. The caller side
              // can trim the top-level alias if it's safe to do so. Since we will call
              // CleanupAliases later in Analyzer, trim non top-level unnecessary alias is safe.
              case Alias(child, _) if !isTopLevel => child
              case other => other
            }.getOrElse(u)
          }
          logDebug(s"Resolving $u to $result")
          result

        // Re-resolves `TempResolvedColumn` if it has tried to be resolved with Aggregate
        // but failed. If we still can't resolve it, we should keep it as `TempResolvedColumn`,
        // so that it won't become a fresh `TempResolvedColumn` again.
        case t: TempResolvedColumn if t.hasTried => withPosition(t) {
          innerResolve(UnresolvedAttribute(t.nameParts), isTopLevel) match {
            case _: UnresolvedAttribute => t
            case other => other
          }
        }

        case u@UnresolvedExtractValue(child, fieldName) =>
          val newChild = innerResolve(child, isTopLevel = false)
          if (newChild.resolved) {
            ExtractValue(newChild, fieldName, conf.resolver)
          } else {
            u.copy(child = newChild)
          }

        case _ => e.mapChildren(innerResolve(_, isTopLevel = false))
      }
      resolved.copyTagsFrom(e)
      resolved
    }

    try {
      val resolved = innerResolve(expr, isTopLevel = true)
      if (allowOuter) resolveOuterRef(resolved) else resolved
    } catch {
      case ae: AnalysisException if !throws =>
        logDebug(ae.getMessage)
        expr
    }
  }


  private def resolveLiteralFunction(nameParts: Seq[String]): Option[NamedExpression] = {
    if (nameParts.length != 1) return None
    val name = nameParts.head
    literalFunctions.find(func => caseInsensitiveResolution(func._1, name)).map {
      case (_, getFuncExpr, getAliasName) =>
        val funcExpr = getFuncExpr()
        Alias(funcExpr, getAliasName(funcExpr))()
    }
  }

  private val literalFunctions: Seq[(String, () => Expression, Expression => String)] = Seq(
    (CurrentDate().prettyName, () => CurrentDate(), toPrettySQL(_)),
    (CurrentTimestamp().prettyName, () => CurrentTimestamp(), toPrettySQL(_)),
    (CurrentUser().prettyName, () => CurrentUser(), toPrettySQL),
    ("user", () => CurrentUser(), toPrettySQL),
    (VirtualColumn.hiveGroupingIdName, () => GroupingID(Nil), _ => VirtualColumn.hiveGroupingIdName)
  )
}

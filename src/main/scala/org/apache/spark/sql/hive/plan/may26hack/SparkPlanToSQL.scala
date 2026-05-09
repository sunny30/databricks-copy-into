package org.apache.spark.sql.hive.plan.may26hack

import org.apache.spark.sql.catalyst.expressions.{CaseKeyWhen, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Converts a Spark 3.5.0 analyzed / optimized LogicalPlan to a SQL string.
 *
 * Usage:
 *   val sql = new SparkPlanToSQL().toSQL(df.queryExecution.analyzed)
 *   // or one-shot via companion object:
 *   val sql = SparkPlanToSQL.toSQL(df.queryExecution.analyzed)
 *
 * This is a class (not object) because CTE handling requires stateful id->name
 * lookup: CTERelationRef carries only a Long id, not the CTE name string.
 * The companion object creates a fresh instance per call.
 *
 * Spark 3.5.0 compatibility (8 issues fixed vs initial version):
 *  #1  WithCTE/CTERelationDef — correct field access via direct type match;
 *      CTERelationDef.child is SubqueryAlias(name, innerPlan) — name lives there
 *  #2  UnaryMinus(child, failOnError) — second wildcard added in frameBound
 *  #3  StringTrim/StringTrimLeft/StringTrimRight — correct Spark 3.5 class names
 *  #4  EvalMode — imported explicitly from Cast.EvalMode
 *  #5  Wildcard() removed — never appears in analyzed plans
 *  #7  ApproximatePercentile — 5-arg pattern (child, pct, acc, _, _)
 *  #8  CTERelationRef.cteId: Long — looked up via cteIdToName map
 */
class SparkPlanToSQL {

  // Populated when WithCTE is visited; consulted by CTERelationRef leaves
  private val cteIdToName = scala.collection.mutable.HashMap.empty[Long, String]

  // ── Plan ──────────────────────────────────────────────────────────────────────

  def toSQL(plan: LogicalPlan): String = plan match {

    // ── WITH / CTE ──────────────────────────────────────────────────────────────
    // Spark 3.5 structure:
    //   WithCTE(plan: LogicalPlan, cteDefs: Seq[CTERelationDef])
    //   CTERelationDef(child: LogicalPlan, id: Long)
    //     child is SubqueryAlias(cteName, innerPlan)
    //   CTERelationRef(cteId: Long, _resolved, output, isStreaming)  <- NO cteName field
    case w: WithCTE =>
      w.cteDefs.foreach { cteDef =>
        val name = cteDef.child match {
          case SubqueryAlias(ident, _) => ident.name
          case _ => s"cte_${cteDef.id}"
        }
        cteIdToName(cteDef.id) = name
      }
      val cteSQL = w.cteDefs.map { cteDef =>
        val name = cteIdToName(cteDef.id)
        val innerPlan = cteDef.child match {
          case SubqueryAlias(_, inner) => inner
          case other => other
        }
        s"$name AS (  ${toSQL(innerPlan).replace("\n", "\n  ")})"
      }.mkString(", ")
      s"WITH $cteSQL ${toSQL(w.plan)}"

    // CTERelationRef — reference to a previously defined CTE; look up name by id
    case ref: CTERelationRef =>
      cteIdToName.getOrElse(ref.cteId, s"/* CTERef(${ref.cteId}) */")

    // ── Project / SELECT ─────────────────────────────────────────────────────────
    case Project(projectList, child) =>
      val cols = projectList.map(namedExprToSQL).mkString(", ")
      child match {
        case _: OneRowRelation => s"SELECT $cols"
        case _:Project | _:Filter =>
          s"SELECT $cols FROM ( SELECT * FROM ( ${childSQL(child)} ))"
        case _: Window => s"SELECT  $cols FROM ( ${toSQL(child).replace("\n", "\n  ")})"

        case _ =>
          s"SELECT $cols FROM ${childSQL(child)}"
      }

    // ── Filter / WHERE / HAVING ──────────────────────────────────────────────────
    case Filter(condition, agg: Aggregate) =>
      s"${toSQL(agg)} HAVING ${exprToSQL(condition)}"
    case Filter(condition, w: Window) =>
      // QUALIFY is not standard SQL; emit as outer WHERE on subquery
      s"SELECT * FROM (  ${toSQL(w).replace("\n", "\n  ")}) WHERE ${exprToSQL(condition)}"
    case Filter(condition, leafNode: LeafNode) =>
      s"SELECT * FROM ${toSQL(leafNode).replace("\n", "\n  ")} WHERE ${exprToSQL(condition)}"
    case Filter(condition, child) =>
      s"SELECT * FROM (  ${toSQL(child).replace("\n", "\n  ")}) WHERE ${exprToSQL(condition)}"
     // s"${childSQL(child)} WHERE ${exprToSQL(condition)}"

    // ── Aggregate / GROUP BY ─────────────────────────────────────────────────────
    case Aggregate(groupingExprs, aggregateExprs, child) =>
      val cols = aggregateExprs.map(namedExprToSQL).mkString(", ")
      val groupBy = if (groupingExprs.isEmpty) ""
      else s" GROUP BY ${groupingExprs.map(exprToSQL).mkString(", ")}"
      s"SELECT $cols FROM ${childSQL(child)} $groupBy"

    // ── Window plan node ─────────────────────────────────────────────────────────
    case w: Window =>
      val winCols = w.windowExpressions.map(namedExprToSQL).mkString(", ") // ← separator changed
      val baseCols = w.child.output.map(_.name).mkString(", ") // ← separator changed
      val allCols = if (baseCols.isEmpty) winCols else s"$baseCols,  $winCols"
      s"SELECT $allCols FROM ${childSQL(w.child)}"

    // ── Join ─────────────────────────────────────────────────────────────────────
    case Join(left, right, joinType, condition, _) =>
      val joinStr = joinTypeToSQL(joinType)
      val on = condition.map(c => s"   ON ${exprToSQL(c)}").getOrElse("")
      s"${childSQL(left)} $joinStr JOIN ${childSQL(right)} $on"

    // ── Sort / ORDER BY ──────────────────────────────────────────────────────────
    case Sort(order, _, child) =>
      val orderStr = order.map { o =>
        val dir = if (o.direction == Ascending) "ASC" else "DESC"
        val nul = o.nullOrdering match {
          case NullsFirst => " NULLS FIRST"
          case NullsLast => " NULLS LAST"
        }
        s"${exprToSQL(o.child)} $dir$nul"
      }.mkString(", ")
      s"${toSQL(child)} ORDER BY $orderStr"

    // ── Limit / Offset ───────────────────────────────────────────────────────────
    case GlobalLimit(le, LocalLimit(_, child)) =>
      val inner = child match {
        case Filter(cond, rel: LogicalRelation) =>
          s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
        // Filter directly over table — emit as SELECT * FROM table WHERE cond
        case Filter(cond, rel: DataSourceV2Relation) =>
          s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
        case Filter(cond, rel: UnresolvedRelation) =>
          s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
        case other => toSQL(other)
      }
      s"$inner\nLIMIT ${exprToSQL(le)}"
    case GlobalLimit(le, child) => val inner = child match {
      case Filter(cond, rel: LogicalRelation) =>
        s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
      // Filter directly over table — emit as SELECT * FROM table WHERE cond
      case Filter(cond, rel: DataSourceV2Relation) =>
        s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
      case Filter(cond, rel: UnresolvedRelation) =>
        s"SELECT * FROM ${toSQL(rel)}\nWHERE ${exprToSQL(cond)}"
      case other => toSQL(other)
    }
      s"$inner\nLIMIT ${exprToSQL(le)}"
    case LocalLimit(le, child) => s"${toSQL(child)} LIMIT ${exprToSQL(le)}"
    case Limit(le, child) => s"${toSQL(child)} LIMIT ${exprToSQL(le)}"
    case Offset(oe, child) => s"${toSQL(child)} OFFSET ${exprToSQL(oe)}"

    // ── Set operations ───────────────────────────────────────────────────────────
    case Union(children, _, _) =>
      children.map(c => s"(  ${toSQL(c).replace("\n", "\n  ")}\n)").mkString("\nUNION ALL\n")
    case Intersect(left, right, isAll) =>
      val kw = if (isAll) "INTERSECT ALL" else "INTERSECT"
      s"(${toSQL(left)}) $kw (${toSQL(right)})"
    case Except(left, right, isAll) =>
      val kw = if (isAll) "EXCEPT ALL" else "EXCEPT"
      s"(${toSQL(left)}) $kw (${toSQL(right)})"

    // ── Distinct ─────────────────────────────────────────────────────────────────
    case Distinct(child) =>
      toSQL(child).replaceFirst("SELECT ", "SELECT DISTINCT ")

    // ── SubqueryAlias ─────────────────────────────────────────────────────────────
    case SubqueryAlias(identifier, child) =>
      child match {
        case _: DataSourceV2Relation => s"${toSQL(child)} AS ${identifier.name}"
        case _ => s"(\n  ${toSQL(child)}) AS ${identifier.name}"
      }

    // ── Table references ──────────────────────────────────────────────────────────
    case UnresolvedRelation(parts, _, _) => parts.mkString(".")
    case relation: DataSourceV2Relation => relation.name
    case relation: org.apache.spark.sql.execution.datasources.LogicalRelation =>
      relation.catalogTable.map(_.identifier.quotedString).getOrElse("/* LogicalRelation */")

    // ── LocalRelation (inline VALUES) ─────────────────────────────────────────────
    case LocalRelation(output, rows, _) =>
      if (rows.isEmpty)
        s"SELECT ${output.map(a => s"CAST(NULL AS ${a.dataType.sql}) AS ${a.name}").mkString(", ")}"
      else {
        val vals = rows.map { row =>
          s"(${output.indices.map(i => literalValueToSQL(row.get(i, output(i).dataType), output(i).dataType)).mkString(", ")})"
        }.mkString(", ")
        s"VALUES $vals"
      }

    case OneRowRelation() => ""

    case other =>
      s"/* ${other.getClass.getSimpleName} */\n${scala.util.Try(other.treeString.take(200)).getOrElse("")}"
  }

  // ── Named expression (SELECT list) ───────────────────────────────────────────

  private def namedExprToSQL(expr: NamedExpression): String = expr match {
    case Alias(WindowExpression(wf, ws), name) =>
      s"${windowFuncToSQL(wf)} OVER ${windowSpecToSQL(ws)} AS ${quoteAlias((name))}"
    case Alias(child, name) =>
      s"${exprToSQL(child)} AS ${quoteAlias(name)}"
    case a: Attribute =>
      a.name
    case other =>
      exprToSQL(other)
  }

  // ── Window function ───────────────────────────────────────────────────────────

  private def windowFuncToSQL(func: Expression): String = func match {
    case RowNumber() => "ROW_NUMBER()"
    case Rank(_) => "RANK()"
    case DenseRank(_) => "DENSE_RANK()"
    case PercentRank(_) => "PERCENT_RANK()"
    case CumeDist() => "CUME_DIST()"
    case NTile(n) => s"NTILE(${exprToSQL(n)})"
    case Lag(input, offset, default, _) =>
      val d = default match {
        case Literal(null, _) => "";
        case d => s", ${exprToSQL(d)}"
      }
      s"LAG(${exprToSQL(input)}, ${exprToSQL(offset)}$d)"
    case Lead(input, offset, default, _) =>
      val d = default match {
        case Literal(null, _) => "";
        case d => s", ${exprToSQL(d)}"
      }
      s"LEAD(${exprToSQL(input)}, ${exprToSQL(offset)}$d)"
    case e if e.getClass.getSimpleName == "FirstValue" =>
      val ignoreNull = e.children.lift(1).exists(_ == Literal(true))
      s"FIRST_VALUE(${exprToSQL(e.children.head)}${if (ignoreNull) " IGNORE NULLS" else ""})"

    case e if e.getClass.getSimpleName == "LastValue" =>
      val ignoreNull = e.children.lift(1).exists(_ == Literal(true))
      s"LAST_VALUE(${exprToSQL(e.children.head)}${if (ignoreNull) " IGNORE NULLS" else ""})"

    case e if e.getClass.getSimpleName == "NthValue" =>
      val offset = e.children.lift(1).map(exprToSQL).getOrElse("1")
      s"NTH_VALUE(${exprToSQL(e.children.head)}, $offset)"
    case NthValue(input, offset, _) =>
      s"NTH_VALUE(${exprToSQL(input)}, ${exprToSQL(offset)})"
    case agg: AggregateFunction => aggFuncToSQL(agg)
    case other => exprToSQL(other)
  }

  // ── Window spec ───────────────────────────────────────────────────────────────

  private def windowSpecToSQL(spec: WindowSpecDefinition): String = {
    val part = if (spec.partitionSpec.isEmpty) ""
    else "PARTITION BY " + spec.partitionSpec.map(exprToSQL).mkString(", ")
    val order = if (spec.orderSpec.isEmpty) ""
    else "ORDER BY " + spec.orderSpec.map { o =>
      s"${exprToSQL(o.child)} ${if (o.direction == Ascending) "ASC" else "DESC"}"
    }.mkString(", ")
    val frame = spec.frameSpecification match {
      case SpecifiedWindowFrame(ft, lo, hi) =>
        s"${if (ft == RowFrame) "ROWS" else "RANGE"} BETWEEN ${frameBound(lo)} AND ${frameBound(hi)}"
      case UnspecifiedFrame => ""
    }
    val parts = Seq(part, order, frame).filter(_.nonEmpty)
    if (parts.isEmpty) "()" else s"(${parts.mkString(" ")})"
  }

  // Fix #2: UnaryMinus in Spark 3.5 = UnaryMinus(child, failOnError: Boolean)
  private def frameBound(b: Expression): String = b match {
    case UnboundedPreceding => "UNBOUNDED PRECEDING"
    case UnboundedFollowing => "UNBOUNDED FOLLOWING"
    case CurrentRow => "CURRENT ROW"
    case Literal(n, _) if n != null => s"$n PRECEDING"
    case UnaryMinus(Literal(n, _), _) => s"$n FOLLOWING" // Fix #2
    case other => exprToSQL(other)
  }

  // ── Aggregate function ────────────────────────────────────────────────────────

  private def aggFuncToSQL(agg: AggregateFunction): String = agg match {
    case Count(Nil) => "COUNT(*)"
    case Count(Seq(Literal(1, _))) => "COUNT(*)" // Fix #5: no Wildcard()
    case Count(children) => s"COUNT(${children.map(exprToSQL).mkString(", ")})"
    case Sum(child, _) => s"SUM(${exprToSQL(child)})"
    case Average(child, _) => s"AVG(${exprToSQL(child)})"
    case Min(child) => s"MIN(${exprToSQL(child)})"
    case Max(child) => s"MAX(${exprToSQL(child)})"
    case First(child, ig) =>
      s"FIRST(${exprToSQL(child)}${if (ig == Literal(true)) " IGNORE NULLS" else ""})"
    case Last(child, ig) =>
      s"LAST(${exprToSQL(child)}${if (ig == Literal(true)) " IGNORE NULLS" else ""})"
    case CollectList(child, _, _) => s"COLLECT_LIST(${exprToSQL(child)})"
    case CollectSet(child, _, _) => s"COLLECT_SET(${exprToSQL(child)})"
    // Fix #7: 5-arg pattern — mutableBufOffset + inputBufOffset added in Spark 3.x
    case ApproximatePercentile(child, pct, acc, _, _) =>
      s"PERCENTILE_APPROX(${exprToSQL(child)}, ${exprToSQL(pct)}, ${exprToSQL(acc)})"
    case HyperLogLogPlusPlus(child, rsd, _, _) =>
      s"APPROX_COUNT_DISTINCT(${exprToSQL(child)}, $rsd)"
    case StddevPop(child, _) => s"STDDEV_POP(${exprToSQL(child)})"
    case StddevSamp(child, _) => s"STDDEV(${exprToSQL(child)})"
    case VariancePop(child, _) => s"VAR_POP(${exprToSQL(child)})"
    case VarianceSamp(child, _) => s"VAR_SAMP(${exprToSQL(child)})"
    case Corr(l, r, _) => s"CORR(${exprToSQL(l)}, ${exprToSQL(r)})"
    case CovPopulation(l, r, _) => s"COVAR_POP(${exprToSQL(l)}, ${exprToSQL(r)})"
    case CovSample(l, r, _) => s"COVAR_SAMP(${exprToSQL(l)}, ${exprToSQL(r)})"
    case Skewness(child, _) => s"SKEWNESS(${exprToSQL(child)})"
    case Kurtosis(child, _) => s"KURTOSIS(${exprToSQL(child)})"
    case other =>
      s"${other.prettyName}(${other.children.map(exprToSQL).mkString(", ")})"
  }

  // ── Expression ────────────────────────────────────────────────────────────────

  def exprToSQL(expr: Expression): String = expr match {

    case a: AttributeReference => a.name
    case a: Attribute => a.name
    case UnresolvedAttribute(parts) => parts.mkString(".")
    case Alias(child, name) => s"${exprToSQL(child)} AS ${quoteAlias(name)}"

    // Literals
    case Literal(null, _) => "NULL"
    case Literal(v, StringType) => s"'${v.toString.replace("'", "''")}'"
    case Literal(v, BooleanType) => v.toString.toUpperCase
    case Literal(v, DateType) => s"DATE '$v'"
    case Literal(v, TimestampType) => s"TIMESTAMP '$v'"
    case Literal(v, _) if v != null &&
      v.getClass.getSimpleName == "CalendarInterval" => s"INTERVAL '$v'"
    case Literal(v, _) => if (v == null) "NULL" else v.toString

    // Arithmetic
    case Add(l, r, _) => s"(${exprToSQL(l)} + ${exprToSQL(r)})"
    case Subtract(l, r, _) => s"(${exprToSQL(l)} - ${exprToSQL(r)})"
    case Multiply(l, r, _) => s"(${exprToSQL(l)} * ${exprToSQL(r)})"
    case Divide(l, r, _) => s"(${exprToSQL(l)} / ${exprToSQL(r)})"
    case Remainder(l, r, _) => s"(${exprToSQL(l)} % ${exprToSQL(r)})"
    case UnaryMinus(child, _) => s"-(${exprToSQL(child)})"
    case Abs(child, _) => s"ABS(${exprToSQL(child)})"

    // Comparison
    case EqualTo(l, r) => s"${exprToSQL(l)} = ${exprToSQL(r)}"
    case EqualNullSafe(l, r) => s"${exprToSQL(l)} <=> ${exprToSQL(r)}"
    case Not(EqualTo(l, r)) => s"${exprToSQL(l)} <> ${exprToSQL(r)}"
    case GreaterThan(l, r) => s"${exprToSQL(l)} > ${exprToSQL(r)}"
    case GreaterThanOrEqual(l, r) => s"${exprToSQL(l)} >= ${exprToSQL(r)}"
    case LessThan(l, r) => s"${exprToSQL(l)} < ${exprToSQL(r)}"
    case LessThanOrEqual(l, r) => s"${exprToSQL(l)} <= ${exprToSQL(r)}"

    // Logical
    case And(l, r) => s"(${exprToSQL(l)} AND ${exprToSQL(r)})"
    case Or(l, r) => s"(${exprToSQL(l)} OR ${exprToSQL(r)})"
    case Not(child) => s"NOT (${exprToSQL(child)})"

    // Null
    case IsNull(child) => s"${exprToSQL(child)} IS NULL"
    case IsNotNull(child) => s"${exprToSQL(child)} IS NOT NULL"
    case IsNaN(child) => s"ISNAN(${exprToSQL(child)})"

    // Collections
    case In(value, list) =>
      s"${exprToSQL(value)} IN (${list.map(exprToSQL).mkString(", ")})"
    case Not(In(value, list)) =>
      s"${exprToSQL(value)} NOT IN (${list.map(exprToSQL).mkString(", ")})"
    case InSet(value, hset) =>
      s"${exprToSQL(value)} IN (${hset.toSeq.map(v => literalValueToSQL(v, value.dataType)).mkString(", ")})"
    case Not(InSet(value, hset)) =>
      s"${exprToSQL(value)} NOT IN (${hset.toSeq.map(v => literalValueToSQL(v, value.dataType)).mkString(", ")})"

    // String
    case Like(l, r, _) => s"${exprToSQL(l)} LIKE ${exprToSQL(r)}"
    case Not(Like(l, r, _)) => s"${exprToSQL(l)} NOT LIKE ${exprToSQL(r)}"
    case RLike(l, r) => s"${exprToSQL(l)} RLIKE ${exprToSQL(r)}"
    case StartsWith(l, r) => s"${exprToSQL(l)} LIKE CONCAT(${exprToSQL(r)}, '%')"
    case EndsWith(l, r) => s"${exprToSQL(l)} LIKE CONCAT('%', ${exprToSQL(r)})"
    case Contains(l, r) => s"${exprToSQL(l)} LIKE CONCAT('%', ${exprToSQL(r)}, '%')"
    case Concat(children) => s"CONCAT(${children.map(exprToSQL).mkString(", ")})"
    case Upper(child) => s"UPPER(${exprToSQL(child)})"
    case Lower(child) => s"LOWER(${exprToSQL(child)})"
    case Length(child) => s"LENGTH(${exprToSQL(child)})"
    // Fix #3: correct Spark 3.5 internal class names
    case StringTrim(child, None) => s"TRIM(${exprToSQL(child)})"
    case StringTrimLeft(child, None) => s"LTRIM(${exprToSQL(child)})"
    case StringTrimRight(child, None) => s"RTRIM(${exprToSQL(child)})"
    case StringReplace(s, f, r) =>
      s"REPLACE(${exprToSQL(s)}, ${exprToSQL(f)}, ${exprToSQL(r)})"
    case Substring(str, pos, len) =>
      s"SUBSTRING(${exprToSQL(str)}, ${exprToSQL(pos)}, ${exprToSQL(len)})"
    case SubstringIndex(str, d, cnt) =>
      s"SUBSTRING_INDEX(${exprToSQL(str)}, ${exprToSQL(d)}, ${exprToSQL(cnt)})"
    case StringLPad(str, len, pad) =>
      s"LPAD(${exprToSQL(str)}, ${exprToSQL(len)}, ${exprToSQL(pad)})"
    case StringRPad(str, len, pad) =>
      s"RPAD(${exprToSQL(str)}, ${exprToSQL(len)}, ${exprToSQL(pad)})"

    // Fix #4: EvalMode imported explicitly
    case Cast(child, dt, _, EvalMode.TRY) => s"TRY_CAST(${exprToSQL(child)} AS ${dt.sql})"
    case Cast(child, dt, _, _) => s"CAST(${exprToSQL(child)} AS ${dt.sql})"

    // Interval / date arithmetic
    case e if e.getClass.getSimpleName == "TimeAdd" =>
      val ch = e.children; s"${exprToSQL(ch(0))} + ${exprToSQL(ch(1))}"
    case e if e.getClass.getSimpleName == "MakeInterval" =>
      scala.util.Try(e.sql).getOrElse("INTERVAL")

    // Conditional
    case If(pred, t, f) =>
      s"IF(${exprToSQL(pred)}, ${exprToSQL(t)}, ${exprToSQL(f)})"
    case CaseWhen(branches, elseVal) =>
      val w = branches.map { case (c, r) =>
        s"WHEN ${exprToSQL(c)} THEN ${exprToSQL(r)}"
      }.mkString(" ")
      s"CASE $w${elseVal.map(e => s" ELSE ${exprToSQL(e)}").getOrElse("")} END"
    //    case CaseKeyWhen(key, branches, elseVal) =>
    //      val w = branches.map { case (c, r) =>
    //        s"WHEN ${exprToSQL(c)} THEN ${exprToSQL(r)}"
    //      }.mkString(" ")
    //      s"CASE ${exprToSQL(key)} $w${elseVal.map(e => s" ELSE ${exprToSQL(e)}").getOrElse("")} END"
    case Coalesce(children) =>
      s"COALESCE(${children.map(exprToSQL).mkString(", ")})"
    case NaNvl(l, r) => s"NANVL(${exprToSQL(l)}, ${exprToSQL(r)})"
    case Least(children) => s"LEAST(${children.map(exprToSQL).mkString(", ")})"
    case Greatest(children) => s"GREATEST(${children.map(exprToSQL).mkString(", ")})"

    // Aggregate expression wrapper
    case AggregateExpression(aggFunc, _, isDistinct, filter, _) =>
      val base = aggFunc match {
        case Count(Seq(Literal(1, _))) if isDistinct => "COUNT(DISTINCT *)"
        case Count(children) if isDistinct =>
          s"COUNT(DISTINCT ${children.map(exprToSQL).mkString(", ")})"
        case other => aggFuncToSQL(other)
      }
      s"$base${filter.map(f => s" FILTER (WHERE ${exprToSQL(f)})").getOrElse("")}"

    // Window expression
    case WindowExpression(wf, ws) =>
      s"${windowFuncToSQL(wf)} OVER ${windowSpecToSQL(ws)}"

    // Math
    case Sqrt(child) => s"SQRT(${exprToSQL(child)})"
    case Floor(child) => s"FLOOR(${exprToSQL(child)})"
    case Ceil(child) => s"CEIL(${exprToSQL(child)})"
    case Round(child, scale, _) => s"ROUND(${exprToSQL(child)}, ${exprToSQL(scale)})"
    case Log(child) => s"LN(${exprToSQL(child)})"
    case Log2(child) => s"LOG2(${exprToSQL(child)})"
    case Log10(child) => s"LOG10(${exprToSQL(child)})"
    case Pow(l, r) => s"POWER(${exprToSQL(l)}, ${exprToSQL(r)})"
    case Signum(child) => s"SIGN(${exprToSQL(child)})"

    // Date/time
    case CurrentDate(v) => "CURRENT_DATE()"
    case CurrentTimestamp() => "CURRENT_TIMESTAMP()"
    case DateAdd(start, days) => s"DATE_ADD(${exprToSQL(start)}, ${exprToSQL(days)})"
    case DateSub(start, days) => s"DATE_SUB(${exprToSQL(start)}, ${exprToSQL(days)})"
    case DateDiff(end, start) => s"DATEDIFF(${exprToSQL(end)}, ${exprToSQL(start)})"
    case Year(child) => s"YEAR(${exprToSQL(child)})"
    case Month(child) => s"MONTH(${exprToSQL(child)})"
    case DayOfMonth(child) => s"DAY(${exprToSQL(child)})"
    case Hour(child, _) => s"HOUR(${exprToSQL(child)})"
    case Minute(child, _) => s"MINUTE(${exprToSQL(child)})"
    case Second(child, _) => s"SECOND(${exprToSQL(child)})"
    case DateFormatClass(ts, fmt, _) =>
      s"DATE_FORMAT(${exprToSQL(ts)}, ${exprToSQL(fmt)})"
    case FromUnixTime(sec, fmt, _) =>
      s"FROM_UNIXTIME(${exprToSQL(sec)}, ${exprToSQL(fmt)})"
    case UnixTimestamp(ts, fmt, _, _) =>
      s"UNIX_TIMESTAMP(${exprToSQL(ts)}, ${exprToSQL(fmt)})"

    // Subqueries
    case ScalarSubquery(plan, _, _, _, _, _) => s"(${toSQL(plan)})"
    case Exists(plan, _, _, _, _) => s"EXISTS (${toSQL(plan)})"

    // UDFs
    case ScalaUDF(_, _, children, _, nameOpt, _, _, _) =>
      s"${nameOpt.getOrElse("udf")}(${children.map(exprToSQL).mkString(", ")})"
    case UnresolvedFunction(parts, args, isDistinct, _, _) =>
      val dist = if (isDistinct) "DISTINCT " else ""
      s"${parts.mkString(".")}($dist${args.map(exprToSQL).mkString(", ")})"

    case _: Star => "*"



    case other =>
      if(other.isInstanceOf[ToPrettyString]){
        exprToSQL(other.asInstanceOf[ToPrettyString].child)
      }else {
        scala.util.Try(other.sql).getOrElse(s"/* ${other.getClass.getSimpleName} */")
      }
  }

  // ── Helpers ────────────────────────────────────────────────────────────────────

  private def childSQL(plan: LogicalPlan): String = plan match {
    case _: UnresolvedRelation => toSQL(plan)
    case _: DataSourceV2Relation => toSQL(plan)
    case _: CTERelationRef => toSQL(plan)
    case s: SubqueryAlias => toSQL(s)
    case _: OneRowRelation => toSQL(plan)
    case l:LogicalRelation => toSQL(l)
    case _ => s" ${toSQL(plan).replace("\n", "\n  ")}"
  }

  private def joinTypeToSQL(jt: JoinType): String = jt match {
    case Inner => "INNER"
    case LeftOuter => "LEFT OUTER"
    case RightOuter => "RIGHT OUTER"
    case FullOuter => "FULL OUTER"
    case Cross => "CROSS"
    case LeftSemi => "LEFT SEMI"
    case LeftAnti => "LEFT ANTI"
    case NaturalJoin(base) => s"NATURAL ${joinTypeToSQL(base)}"
    case UsingJoin(base, _) => joinTypeToSQL(base)
    case _ => "JOIN"
  }

  private def literalValueToSQL(value: Any, dataType: DataType): String =
    if (value == null) "NULL"
    else dataType match {
      case StringType => s"'${value.toString.replace("'", "''")}'"
      case DateType => s"DATE '$value'"
      case TimestampType => s"TIMESTAMP '$value'"
      case BooleanType => value.toString.toUpperCase
      case _ => value.toString
    }

  private def quoteAlias(name1: String): String = {
    val name = stripOuterParens(name1)
    val needsQuoting = name.isEmpty ||
      name.contains(' ') ||
      name.contains('.') ||
      name.contains('-') ||
      name.contains('(') ||
      name.contains(')') ||
      name.contains(',') ||
      name.contains('+') ||
      name.contains('*') ||
      name.contains('/') ||
      name.contains('%') ||
      name.contains('\'') ||
      name.contains('"') ||
      name.contains('`') ||
      name.contains('[') ||
      name.contains(']') ||
      sqlReservedWords.contains(name.toUpperCase)
    if (needsQuoting) s"`$name`" else name
  }


  private val sqlReservedWords = Set(
    "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
    "ON", "AS", "IN", "IS", "NOT", "NULL", "AND", "OR", "LIKE",
    "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
    "DISTINCT", "ALL", "UNION", "INTERSECT", "EXCEPT", "LIMIT",
    "OFFSET", "WITH", "TABLE", "VIEW", "INDEX", "CREATE", "DROP",
    "ALTER", "INSERT", "UPDATE", "DELETE", "MERGE", "INTO", "VALUES",
    "SET", "TRUE", "FALSE", "ASC", "DESC", "NULLS", "FIRST", "LAST",
    "OVER", "PARTITION", "ROWS", "RANGE", "UNBOUNDED", "PRECEDING",
    "FOLLOWING", "CURRENT", "ROW", "FILTER", "WITHIN", "CAST",
    "INTERVAL", "DATE", "TIMESTAMP", "TIME", "YEAR", "MONTH", "DAY"
  )

  private def stripOuterParens(st: String): String = {
    val s = st.replace("toprettystring", "")
    if (s.startsWith("(") && s.endsWith(")")) {
      // Verify the opening paren actually matches the closing paren
      // i.e. they are a balanced pair, not "(a) + (b)"
      var depth = 0
      var i = 0
      var outerMatch = true
      while (i < s.length - 1) { // don't check last char yet
        if (s(i) == '(') depth += 1
        else if (s(i) == ')') {
          depth -= 1
          if (depth == 0) {
            outerMatch = false; i = s.length
          } // paren closed before end
        }
        i += 1
      }
      if (outerMatch) stripOuterParens(s.substring(1, s.length - 1).trim)
      else s
    } else s
  }
}

/** One-shot companion — creates a fresh converter per call so CTE state doesn't leak. */
object SparkPlanToSQL {
  def toSQL(plan: LogicalPlan): String     = new SparkPlanToSQL().toSQL(plan)
  def exprToSQL(expr: Expression): String  = new SparkPlanToSQL().exprToSQL(expr)
}
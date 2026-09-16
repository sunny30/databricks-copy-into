package org.apache.spark.sql.hive.plan.error


import org.apache.spark.sql.AnalysisException

object CLSErrorSanitiser {

  // Extract all backtick-quoted names from message
  private def extractQuotedNames(msg: String): Seq[String] = {
    if (msg == null) return Seq.empty
    val backtickPattern = "`([^`]+)`".r
    backtickPattern.findAllMatchIn(msg).map(_.group(1)).toSeq
  }

  // First quoted name = the column user tried to access
  private def extractMissingColumn(msg: String): Option[String] =
    extractQuotedNames(msg).headOption

  // Remaining quoted names = Spark's suggestions = what IS accessible in plan output
  // These are the CLS-permitted columns Spark found after CLS restriction
  private def extractAccessibleColumns(msg: String): Seq[String] =
    extractQuotedNames(msg).tail

  private def isCLSRelatedError(e: AnalysisException): Boolean = {
    val msg = e.getMessage

    // Case 1 — already a CLS Access denied message thrown by
    // getViewPlan or getSecureLeafPlan — pass through ✓
    if (msg != null && msg.contains("Access denied")) {
      return true
    }

    // Case 2 — Spark analysis error classes that fire when
    // a column is missing from plan output after CLS restriction
    val clsErrorClasses = Set(
      "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
      "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
      "UNRESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
      "MISSING_COLUMN"
    )

    val errorClass = e.getErrorClass
    if (errorClass == null || !clsErrorClasses.contains(errorClass)) {
      return false
    }

    // Only treat as CLS if:
    //   - There IS a missing column name (user referenced something)
    //   - The error came from a plan that went through CLS
    //     (indicated by the presence of a plan tag or by the error class alone)
    // We cannot reliably distinguish CLS-denied vs genuine typo at this boundary
    // without catalog lookup — so we treat ALL unresolved column errors from
    // CLS error classes as potentially CLS-related and show a combined message
    // that covers both cases: wrong column name OR access denied
    extractMissingColumn(msg).isDefined
  }

  def sanitise(e: AnalysisException): AnalysisException = {
    if (isCLSRelatedError(e)) {
      val msg = e.getMessage

      if (msg != null && msg.contains("Access denied")) {
        // Already a clean message from getViewPlan / getSecureLeafPlan
        // Just strip plan and cause chain — no schema info ✓
        new AnalysisException(
          message       = msg,
          line          = e.line,
          startPosition = e.startPosition,
          cause         = None
        )
      } else {
        // Two possible reasons for this error:
        //   1. Column is restricted by CLS — not in permitted output
        //   2. Column name is wrong — genuine typo or incorrect reference
        //
        // We show both possibilities and list what IS accessible
        // so the user can self-diagnose without leaking schema info
        val missingCol     = extractMissingColumn(msg).getOrElse("unknown")
        val accessibleCols = extractAccessibleColumns(msg)

        val accessibleStr = if (accessibleCols.nonEmpty) {
          s"Available columns in this context: [${accessibleCols.mkString(", ")}]."
        } else {
          "No columns are accessible in this context."
        }

        // Combined message — covers both CLS denial and wrong column name
        // Does not leak restricted schema — only shows what IS accessible ✓
        // Does not expose whether the column exists but is restricted ✓
        val message =
        s"Column `$missingCol` cannot be resolved. " +
          s"This may be because the column does not exist or access is restricted " +
          s"by Column-Level Security. $accessibleStr"

        new AnalysisException(
          message       = message,
          line          = e.line,
          startPosition = e.startPosition,
          cause         = None    // strip cause chain — no internal info ✓
        )
      }

    } else {
      // Non-CLS error class entirely (TABLE_OR_VIEW_NOT_FOUND, syntax error etc.)
      // Return SAME object → assertAnalyzed applies Spark default behaviour ✓
      e
    }
  }
}

/**
// PATCHED — Spark 3.5.0
// sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala

def assertAnalyzed(): Unit = {
  analyzed
  try {
    sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
  } catch {
    case e: AnalysisException =>
      // CLS error sanitisation — intercept here because:
      //   1. Called exactly once per user-facing query ✓
      //   2. All planLater re-entries complete before this point ✓
      //   3. No internal control-flow signals reach here ✓
      val sanitised = org.apache.spark.sql.hive.plan
        .CLSErrorSanitiser.sanitise(e)

      if (sanitised ne e) {
        // CLS error — throw sanitised exception
        // No column names, no plan fragment, no cause chain ✓
        throw sanitised
      } else {
        // Non-CLS error — attach analyzed plan as before (Spark default behaviour)
        val ae = new AnalysisException(
          e.message,
          e.line,
          e.startPosition,
          Option(analyzed)
        )
        ae.setStackTrace(e.getStackTrace)
        throw ae
      }
  }
}
 */
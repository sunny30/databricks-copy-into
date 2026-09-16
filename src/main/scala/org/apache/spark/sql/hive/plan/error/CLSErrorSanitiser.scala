package org.apache.spark.sql.hive.plan.error


import org.apache.spark.sql.AnalysisException

object CLSErrorSanitiser {

  // Restricted column = does NOT start with cls_ prefix
  private def isRestrictedColumn(colName: String): Boolean =
    !colName.startsWith("cls_")

  // Extract all column names from AnalysisException message
  // Handles backtick-quoted names: `order_id`, `amount`
  private def extractMissingColumns(e: AnalysisException): Seq[String] = {
    val msg = e.getMessage
    if (msg == null) return Seq.empty
    val backtickPattern = "`([^`]+)`".r
    backtickPattern.findAllMatchIn(msg).map(_.group(1)).toSeq
  }

  private def isCLSRelatedError(e: AnalysisException): Boolean = {
    val msg = e.getMessage

    // Case 1 — already a CLS access denied message
    // thrown directly by:
    //   getViewPlan    → "Access denied: no permitted columns in view ..."
    //   getSecureLeafPlan → "Access denied: ..."
    // Pass through as-is — already sanitised, no column names ✓
    if (msg != null && msg.contains("Access denied")) {
      return true
    }

    // Case 2 — Spark analysis error on a restricted column
    // These error classes fire when a restricted column is referenced
    // by the user query after CLS has stripped it from the plan output
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

    // Confirm the missing column is actually a restricted (non-cls_) column
    // This prevents false positives on genuine user typos or missing columns
    val missingCols = extractMissingColumns(e)
    missingCols.nonEmpty && missingCols.exists(isRestrictedColumn)
  }

  // Returns:
  //   new sanitised AnalysisException → if CLS-related (sanitised ne e)
  //   original e unchanged            → if non-CLS (sanitised eq e)
  // assertAnalyzed uses `ne` check to distinguish the two cases
  def sanitise(e: AnalysisException): AnalysisException = {
    if (isCLSRelatedError(e)) {

      if (e.getMessage != null && e.getMessage.contains("Access denied")) {
        // Already a clean CLS message from getViewPlan or getSecureLeafPlan
        // Rewrap to strip plan fragment and cause chain which may leak schema ✓
        new AnalysisException(
          message       = e.getMessage,
          line          = e.line,
          startPosition = e.startPosition,
              // strip plan — no schema structure leaked ✓
          cause         = None     // strip cause chain — no stacktrace info leaked ✓
        )
      } else {
        // Spark UNRESOLVED_COLUMN / MISSING_ATTRIBUTES on restricted column
        // Replace with generic message — no restricted column names ✓
        new AnalysisException(
          message       = "Access denied: insufficient column privileges. " +
            "One or more columns referenced are restricted " +
            "by Column-Level Security.",
          line          = e.line,
          startPosition = e.startPosition,
          cause         = None     // strip cause chain ✓
        )
      }

    } else {
      // Non-CLS error — return SAME object unchanged
      // assertAnalyzed detects this via `sanitised eq e`
      // and applies Spark default behaviour (attach analyzed plan)
      e
    }
  }
}
package org.apache.spark.sql.hive.plan.spark.sql.connector.index

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, EmptyRow, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex, NoopCache, PartitionDirectory, PartitionPath, PartitionSpec}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

/**
 * A custom [[InMemoryFileIndex]] that intercepts Spark's partition inference pipeline
 * and overlays a caller-supplied [[StructType]] onto the inferred partition schema.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  PartitioningAwareFileIndex (abstract)                                  │
 * │    └─ cachedPartitionSpec: PartitionSpec  (lazy val)                    │
 * │         = inferPartitioning()   ◄─── we override this                  │
 * │              └─ PartitionSpec(partitionColumns: StructType,             │
 * │                               partitions: Seq[PartitionDirectory])      │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Strategy
 * ─────────
 * 1. Let super.inferPartitioning() run fully (directory walking, name=value
 *    pair parsing, type inference via PartitionUtils.parsePartitions).
 * 2. If `inputPartitionSchema` is provided:
 *    a. Build a **reconciled schema**: for every column in the inferred
 *       PartitionSpec, replace its DataType (and nullability) with the
 *       corresponding field from `inputPartitionSchema` if the column name
 *       matches (case-insensitive).  Columns absent from the provided schema
 *       are kept as-is (allows partial overrides).
 *    b. Recast each [[PartitionDirectory]]'s [[InternalRow]] so that actual
 *       partition values are cast from the inferred type to the target type
 *       using [[Cast]].eval() in interpreted mode.
 * 3. If `inputPartitionSchema` is None, the result is identical to vanilla
 *    [[InMemoryFileIndex]] behaviour.
 *
 * @param sparkSession          active SparkSession
 * @param rootPaths             root paths for file discovery (same as super)
 * @param parameters            datasource options map (same as super)
 * @param userSpecifiedSchema   optional full table schema forwarded to super
 * @param inputPartitionSchema  the partition-column schema to enforce;
 *                              None means "pure inference, no override"
 * @param fileStatusCache       cache for [[FileStatus]] entries
 */
class PartitionSchemaAwareFileIndex(
                                     sparkSession: SparkSession,
                                     rootPaths: Seq[Path],
                                     parameters: Map[String, String],
                                     userSpecifiedSchema: Option[StructType],
                                     val inputPartitionSchema: Option[StructType],
                                     fileStatusCache: FileStatusCache = NoopCache)
  extends InMemoryFileIndex(
    sparkSession,
    rootPaths,
    parameters,
    userSpecifiedSchema,
    fileStatusCache)
    with Logging {

  // ─────────────────────────────────────────────────────────────────────────
  // Core override – called exactly once via cachedPartitionSpec (lazy val)
  // ─────────────────────────────────────────────────────────────────────────

  override protected def inferPartitioning(): PartitionSpec = {
    val inferredSpec = super.inferPartitioning()

    logInfo(
      s"[PartitionSchemaAwareFileIndex] Inferred partition schema: " +
        s"${inferredSpec.partitionColumns.simpleString}")

    inputPartitionSchema match {
      case None =>
        logInfo("[PartitionSchemaAwareFileIndex] No inputPartitionSchema provided; " +
          "using fully inferred spec.")
        inferredSpec

      case Some(providedSchema) =>
        logInfo(
          s"[PartitionSchemaAwareFileIndex] Applying provided partition schema: " +
            s"${providedSchema.simpleString}")
        applyProvidedSchema(inferredSpec, providedSchema)
    }
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Schema reconciliation
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Reconciles the inferred [[PartitionSpec]] with the caller-supplied schema.
   *
   * Column matching is case-insensitive on the column name.
   * Columns in the inferred spec but NOT in `providedSchema` are kept as-is
   * (partial override semantics).
   * Columns in `providedSchema` but NOT in the inferred spec are silently
   * ignored (inference drives which columns exist; the provided schema only
   * controls types).
   */
  private def applyProvidedSchema(
                                   inferredSpec: PartitionSpec,
                                   providedSchema: StructType): PartitionSpec = {

    // Build a lowercased name → StructField lookup for O(1) access
    val providedFieldMap: Map[String, StructField] =
      providedSchema.fields.map(f => f.name.toLowerCase -> f).toMap

    // ── Step 1: Build reconciled StructType ──────────────────────────────
    val reconciledFields: Array[StructField] =
      inferredSpec.partitionColumns.fields.map { inferredField =>
        providedFieldMap.get(inferredField.name.toLowerCase) match {
          case Some(overrideField) =>
            logDebug(
              s"[PartitionSchemaAwareFileIndex] Column '${inferredField.name}': " +
                s"${inferredField.dataType.simpleString} → " +
                s"${overrideField.dataType.simpleString}")
            // Preserve the inferred column name casing; use provided type + nullability
            inferredField.copy(
              dataType   = overrideField.dataType,
              nullable   = overrideField.nullable,
              metadata   = overrideField.metadata)

          case None =>
            logDebug(
              s"[PartitionSchemaAwareFileIndex] Column '${inferredField.name}' " +
                s"not in provided schema; keeping inferred type " +
                s"${inferredField.dataType.simpleString}")
            inferredField
        }
      }
    val reconciledSchema = StructType(reconciledFields)

    // ── Step 2: Recast every PartitionDirectory row ──────────────────────
    val reconciledPartitions: Seq[PartitionPath] =
      inferredSpec.partitions.map { partDir =>
        val recastRow = recastInternalRow(
          row        = partDir.values,
          fromSchema = inferredSpec.partitionColumns,
          toSchema   = reconciledSchema)
        partDir.copy(values = recastRow)
      }

    logInfo(
      s"[PartitionSchemaAwareFileIndex] Reconciled partition schema: " +
        s"${reconciledSchema.simpleString}  " +
        s"(${reconciledPartitions.size} partitions)")

    PartitionSpec(reconciledSchema, reconciledPartitions)
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Value recasting
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Produces a new [[InternalRow]] where each field value is cast from
   * `fromSchema(i).dataType` to `toSchema(i).dataType`.
   *
   * If the types are identical the value is passed through without
   * allocation.  Nulls propagate unchanged.  Casting is performed via
   * [[Cast]].eval() in interpreted (non-codegen) mode, which is safe in
   * a driver-side context where codegen is not available.
   */
  private def recastInternalRow(
                                 row: InternalRow,
                                 fromSchema: StructType,
                                 toSchema: StructType): InternalRow = {

    require(
      fromSchema.length == toSchema.length,
      s"Schema length mismatch: from=${fromSchema.length} to=${toSchema.length}")

    val recastValues: Array[Any] = fromSchema.fields.zipWithIndex.map {
      case (fromField, idx) =>
        val toField = toSchema.fields(idx)

        if (row.isNullAt(idx)) {
          null
        } else if (fromField.dataType == toField.dataType) {
          // No-op: types are identical, avoid unnecessary work
          row.get(idx, fromField.dataType)
        } else {
          // Build a Cast expression and evaluate it on the driver in
          // interpreted mode.  Cast is not invoked with codegen here;
          // we rely on the fact that Cast.eval() works without a
          // CodegenContext for all primitive/string/date/decimal types
          // that are legal as partition column types.
          val literal = Literal(row.get(idx, fromField.dataType), fromField.dataType)
          val cast    = Cast(literal, toField.dataType,
            Option(sparkSession.sessionState.conf.sessionLocalTimeZone))
          cast.eval(EmptyRow)
        }
    }

    new GenericInternalRow(recastValues)
  }
}

// ── Companion helpers ──────────────────────────────────────────────────────

private object PartitionSchemaAwareFileIndex {

  /** Factory method – mirrors the style of InMemoryFileIndex usage in FileTable */
  def apply(
             sparkSession: SparkSession,
             rootPaths: Seq[Path],
             parameters: Map[String, String],
             userSpecifiedSchema: Option[StructType],
             inputPartitionSchema: Option[StructType]): PartitionSchemaAwareFileIndex =
    new PartitionSchemaAwareFileIndex(
      sparkSession,
      rootPaths,
      parameters,
      userSpecifiedSchema,
      inputPartitionSchema,
      FileStatusCache.getOrCreate(sparkSession))
}

package org.apache.spark.sql.hive.plan.spark.sql.connector.index


import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{
  FileStatusCache,
  InMemoryFileIndex,
  NoopCache,
  PartitionDirectory,
  PartitionSpec
}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * An [[InMemoryFileIndex]] that intercepts [[inferPartitioning]] and overlays
 * a caller-supplied [[StructType]] onto the inferred partition schema.
 *
 * Inference still runs fully (directory walking, name=value parsing).
 * If [[inputPartitionSchema]] is provided, column types and nullability are
 * reconciled against it; columns absent from the provided schema keep their
 * inferred types (partial override semantics).
 *
 * Performance design — Spark 3.5.0
 * ────────────────────────────────
 *  • Cast nodes built ONCE per column, outside the partition loop.
 *  • One GenericInternalRow per cast-column reused across all partitions
 *    (safe: driver-side, single-threaded lazy-val materialisation).
 *  • If no column types differ, the entire partition loop is skipped.
 *  • partDir.copy(values = ...) is used instead of constructing
 *    PartitionDirectory directly:
 *      – avoids touching the second constructor field whose type is
 *        Seq[PartitionPath] in 3.5.0 (not Array/Seq[FileStatus])
 *      – keeps the existing Seq[PartitionPath] reference without any
 *        allocation or type error
 *  • newVals Array is reused across partitions; .clone() only when dirty.
 *  • Inner loop is while + index — no closure / boxing overhead.
 */
class PartitionSchemaAwareFileIndexOpt(
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

  // ── Intercept point ───────────────────────────────────────────────────────
  // Called exactly once; result is memoised in cachedPartitionSpec (lazy val
  // in PartitioningAwareFileIndex).

  override protected def inferPartitioning(): PartitionSpec = {
    val inferred = super.inferPartitioning()
    logInfo(s"[PSAFI] Inferred partition schema: ${inferred.partitionColumns.simpleString}")

    inputPartitionSchema match {
      case None       => inferred
      case Some(prov) =>
        logInfo(s"[PSAFI] Applying provided partition schema: ${prov.simpleString}")
        applyProvidedSchema(inferred, prov)
    }
  }

  // ── Core reconciliation + optimised value recasting ───────────────────────

  private def applyProvidedSchema(
                                   inferredSpec: PartitionSpec,
                                   providedSchema: StructType): PartitionSpec = {

    // O(1) lookup: lowercased name → override StructField
    val overrideMap: Map[String, StructField] =
      providedSchema.fields.map(f => f.name.toLowerCase -> f).toMap

    // ── 1. Build reconciled StructType ────────────────────────────────────
    val reconciledFields: Array[StructField] =
      inferredSpec.partitionColumns.fields.map { inf =>
        overrideMap.get(inf.name.toLowerCase) match {
          case Some(ov) =>
            logDebug(s"[PSAFI] '${inf.name}': " +
              s"${inf.dataType.simpleString} → ${ov.dataType.simpleString}")
            inf.copy(dataType = ov.dataType, nullable = ov.nullable, metadata = ov.metadata)
          case None =>
            inf
        }
      }
    val reconciledSchema = StructType(reconciledFields)

    // ── 2. Hoist Cast expressions — built ONCE per column ─────────────────
    //
    //   casterRows(i)  != null → reusable 1-element row for column i
    //   casterExprs(i) != null → Cast(BoundReference(0, fromType), toType)
    //   both null               → types identical, copy value as-is
    //
    // BoundReference(0, fromType) reads ordinal 0 from the 1-element row we
    // pass on every eval(), avoiding Literal construction per partition.
    //
    // Cast.initializeInternal() does NOT exist in Spark 3.5.0 — removed.
    // Cast in 3.5.0 initialises lazily on first eval() which is fine here.

    val tz         = Option(sparkSession.sessionState.conf.sessionLocalTimeZone)
    val numCols    = reconciledFields.length
    val fromFields = inferredSpec.partitionColumns.fields

    val casterRows  = new Array[GenericInternalRow](numCols)  // null = no cast
    val casterExprs = new Array[Cast](numCols)                // null = no cast
    var anyChanged  = false

    var c = 0
    while (c < numCols) {
      val from = fromFields(c)
      val to   = reconciledFields(c)
      if (from.dataType != to.dataType) {
        casterRows(c)  = new GenericInternalRow(1)
        casterExprs(c) = Cast(
          BoundReference(0, from.dataType, nullable = true),
          to.dataType,
          tz)
        anyChanged = true
      }
      c += 1
    }

    // ── 3. Short-circuit: no column types changed ─────────────────────────
    //
    // Only the schema object changes (nullability / metadata); row values are
    // identical, so we keep the original Seq[PartitionDirectory] as-is.
    if (!anyChanged) {
      logInfo(s"[PSAFI] No type changes needed; returning " +
        s"${inferredSpec.partitions.size} partitions with schema update only.")
      return PartitionSpec(reconciledSchema, inferredSpec.partitions)
    }

    // ── 4. Recast each PartitionDirectory row ─────────────────────────────
    //
    // Key design choice — partDir.copy(values = newRow):
    //
    //   In Spark 3.5.0, PartitionDirectory is:
    //     case class PartitionDirectory(values: InternalRow, files: Seq[PartitionPath])
    //
    //   Constructing fresh requires knowing the second field type and accessing
    //   partDir.files, both of which fail to compile in 3.5.0 because:
    //     • "partDir.files not recognised" — field may be named differently
    //     • "Required Seq[PartitionPath], found Seq[Any]" — type is opaque here
    //
    //   partDir.copy(values = newRow) avoids touching the second field entirely.
    //   The compiler resolves it from partDir's own type at the call site —
    //   no ambiguity, no type error, no field name dependency.

    val newVals = new Array[Any](numCols)   // reused across partitions

    val reconciledPartitions = inferredSpec.partitions.map { partDir =>
      val oldRow   = partDir.values
      var rowDirty = false

      var i = 0
      while (i < numCols) {
        if (oldRow.isNullAt(i)) {
          newVals(i) = null
        } else if (casterExprs(i) == null) {
          // No type change for this column — pass through
          newVals(i) = oldRow.get(i, fromFields(i).dataType)
        } else {
          // Reuse the per-column row: eval() is synchronous, driver-side
          casterRows(i).update(0, oldRow.get(i, fromFields(i).dataType))
          newVals(i) = casterExprs(i).eval(casterRows(i))
          rowDirty   = true
        }
        i += 1
      }

      if (rowDirty) {
        // .copy() only replaces `values`; the second field (Seq[PartitionPath])
        // is carried over from partDir unchanged — no type touch needed.
        partDir.copy(values = new GenericInternalRow(newVals.clone()))
      } else {
        partDir   // zero allocation for unchanged rows
      }
    }

    logInfo(s"[PSAFI] Reconciled ${reconciledPartitions.size} partitions. " +
      s"Schema: ${reconciledSchema.simpleString}")

    PartitionSpec(reconciledSchema, reconciledPartitions)
  }
}

// ── Companion ──────────────────────────────────────────────────────────────

object PartitionSchemaAwareFileIndexOpt {

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
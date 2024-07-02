package org.apache.spark.sql.hive.catalog

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFieldName, UnresolvedFieldPosition}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalog}
import org.apache.spark.sql.catalyst.plans.logical.QualifiedColType
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, ColumnChange, ColumnPosition, DeleteColumn, RemoveProperty, RenameColumn, SetProperty, UpdateColumnComment, UpdateColumnNullability, UpdateColumnPosition, UpdateColumnType}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors}
import org.apache.spark.sql.delta.commands.{AlterTableAddColumnsDeltaCommand, AlterTableAddConstraintDeltaCommand, AlterTableChangeColumnDeltaCommand, AlterTableDropColumnsDeltaCommand, AlterTableDropConstraintDeltaCommand, AlterTableDropFeatureDeltaCommand, AlterTableReplaceColumnsDeltaCommand, AlterTableSetLocationDeltaCommand, AlterTableSetPropertiesDeltaCommand, AlterTableUnsetPropertiesDeltaCommand, TableCreationModes}
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.tablefeatures.DropFeature
import org.apache.spark.sql.hive.plan.delta.UnityCreateDeltaTableCommand
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.hive.catalog.UnityCatalog

import scala.collection.mutable

class UnityDeltaCatalog(plugin: ExternalCatalog) {



  def createDeltaTable(table: CatalogTable):Unit={
    UnityCreateDeltaTableCommand(
      table,
      existingTableOpt = None,
      mode = TableCreationModes.Create.mode,
      query = None
    ).run(SparkSession.active)
    plugin.createTable(table,true)
  }

  def loadTable(ident: Identifier): Table = {
    val tableName = ident.asTableIdentifier.table
    val dbName = ident.asTableIdentifier.database.getOrElse("default")
    val tt = plugin.getTable(table = tableName, db = dbName)
    if (tt == null)
      return null
    if (tt.provider.isDefined && tt.provider.get.equalsIgnoreCase("delta")) {
      DeltaTableV2(
        SparkSession.active,
        new Path(tt.location),
        catalogTable = Some(tt),
        tableIdentifier = Some(ident.toString))
    } else {
      if (tt != null) {
        V1Table(plugin.getTable(table = tableName, db = dbName))
      } else {
        null
      }
    }
  }


   def alterTable(ident: Identifier, changes: Seq[TableChange]): Table =  {
    // We group the table changes by their type, since Delta applies each in a separate action.
    // We also must define an artificial type for SetLocation, since data source V2 considers
    // location just another property but it's special in catalog tables.
    val spark = SparkSession.active
    class SetLocation {}
    val grouped = changes.groupBy {
      case s: SetProperty if s.property() == "location" => classOf[SetLocation]
      case c => c.getClass
    }
    val table = loadTable(ident) match {
      case deltaTable: DeltaTableV2 => deltaTable
      case _ => throw new IllegalArgumentException("only delta is allowed")
    }

    // Whether this is an ALTER TABLE ALTER COLUMN SYNC IDENTITY command.
    var syncIdentity = false
    val columnUpdates = new mutable.HashMap[Seq[String], (StructField, Option[ColumnPosition])]()
    val isReplaceColumnsCommand = grouped.get(classOf[DeleteColumn]) match {
      case Some(deletes) if grouped.contains(classOf[AddColumn]) =>
        // Convert to Seq so that contains method works
        val deleteSet = deletes.asInstanceOf[Seq[DeleteColumn]].map(_.fieldNames().toSeq).toSet
        // Ensure that all the table top level columns are being deleted
        table.schema().fieldNames.forall(f => deleteSet.contains(Seq(f)))
      case _ =>
        false
    }

    if (isReplaceColumnsCommand &&
      SparkSession.active.sessionState.conf.getConf(DeltaSQLConf.DELTA_REPLACE_COLUMNS_SAFE)) {
      // The new schema is essentially the AddColumn operators
      val tableToUpdate = table
      val colsToAdd = grouped(classOf[AddColumn]).asInstanceOf[Seq[AddColumn]]
      val structFields = colsToAdd.map { col =>
        assert(
          col.fieldNames().length == 1, "We don't expect replace to provide nested column adds")
        var field = StructField(col.fieldNames().head, col.dataType, col.isNullable)
        Option(col.comment()).foreach { comment =>
          field = field.withComment(comment)
        }
        Option(col.defaultValue()).foreach { defValue =>
          field = field.withCurrentDefaultValue(defValue.getSql)
        }
        field
      }
      AlterTableReplaceColumnsDeltaCommand(tableToUpdate, structFields).run(spark)
      return loadTable(ident)
    }

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        val tableToUpdate = table
        AlterTableAddColumnsDeltaCommand(
          tableToUpdate,
          newColumns.asInstanceOf[Seq[AddColumn]].map { col =>
            // Convert V2 `AddColumn` to V1 `QualifiedColType` as `AlterTableAddColumnsDeltaCommand`
            // is a V1 command.
            val name = col.fieldNames()
            val path = if (name.length > 1) Some(UnresolvedFieldName(name.init)) else None
            QualifiedColType(
              path,
              name.last,
              col.dataType(),
              col.isNullable,
              Option(col.comment()),
              Option(col.position()).map(UnresolvedFieldPosition),
              Option(col.defaultValue()).map(_.getSql())
            )
          }).run(spark)

      case (t, deleteColumns) if t == classOf[DeleteColumn] =>
        AlterTableDropColumnsDeltaCommand(
          table, deleteColumns.asInstanceOf[Seq[DeleteColumn]].map(_.fieldNames().toSeq)).run(spark)

      case (t, newProperties) if t == classOf[SetProperty] =>
        AlterTableSetPropertiesDeltaCommand(
          table,
          DeltaConfigs.validateConfigurations(
            newProperties.asInstanceOf[Seq[SetProperty]].map { prop =>
              prop.property() -> prop.value()
            }.toMap)
        ).run(spark)

      case (t, oldProperties) if t == classOf[RemoveProperty] =>
        AlterTableUnsetPropertiesDeltaCommand(
          table,
          oldProperties.asInstanceOf[Seq[RemoveProperty]].map(_.property()),
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case (t, columnChanges) if classOf[ColumnChange].isAssignableFrom(t) =>
        def getColumn(fieldNames: Seq[String]): (StructField, Option[ColumnPosition]) = {
          columnUpdates.getOrElseUpdate(fieldNames, {
            // TODO: Theoretically we should be able to fetch the snapshot from a txn.
            val schema = table.initialSnapshot.schema
            val colName = UnresolvedAttribute(fieldNames).name
            val fieldOpt = schema.findNestedField(fieldNames, includeCollections = true,
                spark.sessionState.conf.resolver)
              .map(_._2)
            val field = fieldOpt.getOrElse {
              throw DeltaErrors.nonExistentColumnInSchema(colName, schema.treeString)
            }
            field -> None
          })
        }

        columnChanges.foreach {
          case comment: UpdateColumnComment =>
            val field = comment.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.withComment(comment.newComment()) -> pos

          case dataType: UpdateColumnType =>
            val field = dataType.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(dataType = dataType.newDataType()) -> pos

          case position: UpdateColumnPosition =>
            val field = position.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField -> Option(position.position())

          case nullability: UpdateColumnNullability =>
            val field = nullability.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(nullable = nullability.nullable()) -> pos

          case rename: RenameColumn =>
            val field = rename.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(name = rename.newName()) -> pos


          case other =>
            throw DeltaErrors.unrecognizedColumnChange(s"${other.getClass}")
        }

      case (t, locations) if t == classOf[SetLocation] =>
        if (locations.size != 1) {
          throw DeltaErrors.cannotSetLocationMultipleTimes(
            locations.asInstanceOf[Seq[SetProperty]].map(_.value()))
        }
        if (table.tableIdentifier.isEmpty) {
          throw DeltaErrors.setLocationNotSupportedOnPathIdentifiers()
        }
        AlterTableSetLocationDeltaCommand(
          table,
          locations.head.asInstanceOf[SetProperty].value()).run(spark)

      case (t, constraints) if t == classOf[AddConstraint] =>
        constraints.foreach { constraint =>
          val c = constraint.asInstanceOf[AddConstraint]
          AlterTableAddConstraintDeltaCommand(table, c.constraintName, c.expr).run(spark)
        }

      case (t, constraints) if t == classOf[DropConstraint] =>
        constraints.foreach { constraint =>
          val c = constraint.asInstanceOf[DropConstraint]
          AlterTableDropConstraintDeltaCommand(table, c.constraintName, c.ifExists).run(spark)
        }

      case (t, dropFeature) if t == classOf[DropFeature] =>
        // Only single feature removal is supported.
        val dropFeatureTableChange = dropFeature.head.asInstanceOf[DropFeature]
        val featureName = dropFeatureTableChange.featureName
        val truncateHistory = dropFeatureTableChange.truncateHistory
        AlterTableDropFeatureDeltaCommand(table, featureName, truncateHistory).run(spark)

    }

    columnUpdates.foreach { case (fieldNames, (newField, newPositionOpt)) =>
      AlterTableChangeColumnDeltaCommand(
        table,
        fieldNames.dropRight(1),
        fieldNames.last,
        newField,
        newPositionOpt,
        syncIdentity = syncIdentity).run(spark)
    }

   loadTable(ident)
  }

}

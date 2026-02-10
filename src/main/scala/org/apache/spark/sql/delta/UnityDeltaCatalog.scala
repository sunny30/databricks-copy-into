package org.apache.spark.sql.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFieldName, UnresolvedFieldPosition}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, QualifiedColType}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.delta.catalog.{BucketTransform, DeltaTableV2}
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf, DeltaSourceUtils}
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.tablefeatures.DropFeature
import org.apache.spark.sql.delta.util.PartitionUtils
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}

import scala.collection.JavaConverters._
import org.apache.spark.sql.delta.skipping.clustering.temp.{ClusterByTransform => TempClusterByTransform}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.connector.catalog.TableCapability._

import java.net.URI
import java.sql.Timestamp
import java.util
import java.util.Locale
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable

class UnityDeltaCatalog(plugin: ExternalCatalog, catalogName: String) extends DeltaLogging {


  def createDeltaTable(
                        ident: Identifier,
                        schema: StructType,
                        partitions: Array[Transform],
                        allTableProperties: java.util.Map[String, String],
                        writeOptions: Map[String, String],
                        sourceQuery: Option[DataFrame],
                        operation: TableCreationModes.CreationMode,
                        location: String,
                        isExternal: Boolean
                      ): Table = recordFrameProfile(
    "DeltaCatalog", "createDeltaTable") {

    val tableProperties = allTableProperties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case TableCatalog.PROP_EXTERNAL => false
      case "path" => false
      case "option.path" => false
      case _ => true
    }.toMap
    val (partitionColumns, maybeBucketSpec, maybeClusterBySpec) = convertTransforms(partitions)
    validateClusterBySpec(maybeClusterBySpec, schema)
    var newSchema = schema
    var newPartitionColumns = partitionColumns
    var newBucketSpec = maybeBucketSpec
    val conf = SparkSession.active.sessionState.conf
    allTableProperties.asScala
      .get(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key)
      .foreach(StatisticsCollection.validateDeltaStatsColumns(schema, partitionColumns, _))
    val isByPath = isPathIdentifier(ident)
    if (isByPath && !conf.getConf(DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS)
      && allTableProperties.containsKey("location")
      // The location property can be qualified and different from the path in the identifier, so
      // we check `endsWith` here.
      && Option(allTableProperties.get("location")).exists(!_.endsWith(ident.name()))
    ) {
      throw DeltaErrors.ambiguousPathsInCreateTableException(
        ident.name(), allTableProperties.get("location"))
    }

    val id = {
      TableIdentifier(ident.name(), ident.namespace().lastOption)
    }
    var locUriOpt = new Path(location).toUri
    val existingTableOpt = getExistingTableIfExists(id)
    val loc = locUriOpt
    val storage = DataSource.buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val tableType =
      if (isExternal) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val commentOpt = Option(allTableProperties.get("comment"))


    var tableDesc = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = newSchema,
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = newPartitionColumns,
      bucketSpec = newBucketSpec,
      properties = tableProperties,
      comment = commentOpt
    )

    if (tableType == CatalogTableType.EXTERNAL) {
      val tableLocation = tableDesc.storage.locationUri.get.toString
      import io.delta.tables._

      val persistedTable = if (tableDesc.schema.nonEmpty) {
        tableDesc
      } else {
        val deltaTable = DeltaTable.forPath(tableLocation)
        println(s"the schema of projected table: ${deltaTable.toDF.schema.prettyJson}")
        tableDesc.copy(schema = deltaTable.toDF.schema)
      }
      plugin.createTable(tableDefinition = persistedTable, true)
    } else {
      plugin.createTable(tableDefinition = tableDesc, true)
      //loadTable(ident)
    }
    //just to make sure if there is any specific changes with respect to ocation management
    val tblFromMetastore = plugin.getTable(tableDesc.identifier.database.getOrElse("default"), tableDesc.identifier.table)

    try {
      val withDb =
        verifyTableAndSolidify(
          tblFromMetastore,
          None,
          maybeClusterBySpec
        )

      val writer = sourceQuery.map { df =>
        WriteIntoDelta(
          DeltaLog.forTable(SparkSession.active, new Path(loc)),
          operation.mode,
          new DeltaOptions(withDb.storage.properties, SparkSession.active.sessionState.conf),
          withDb.partitionColumnNames,
          withDb.properties ++ commentOpt.map("comment" -> _),
          df,
          Some(tableDesc),
          schemaInCatalog = if (newSchema != schema) Some(newSchema) else None)
      }

      UnityCreateDeltaTableCommand(
        withDb,
        existingTableOpt,
        operation.mode,
        writer,
        operation,
        tableByPath = isByPath).run(SparkSession.active)


      loadTable(ident)
    }catch {
      case e:Exception =>
       // plugin.dropTable(ident.namespace().lastOption.getOrElse("default"), ident.name(), true, false)
        throw e
    }


  }

  protected def isPathIdentifier(tableIdentifier: TableIdentifier): Boolean = {
    isPathIdentifier(Identifier.of(tableIdentifier.database.toArray, tableIdentifier.table))
  }

  def getExistingTableIfExists(table: TableIdentifier): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself

    if (isPathIdentifier(table)) return None
    val tableExists = plugin.tableExists(table.database.getOrElse("default"), table.table)
    if (tableExists) {
      val oldTable = plugin.getTable(table.database.getOrElse("default"), table.table)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw DeltaErrors.cannotWriteIntoView(table)
      }
      if (!DeltaSourceUtils.isDeltaTable(oldTable.provider)) {
        throw DeltaErrors.notADeltaTable(table.table)
      }
      Some(oldTable)
    } else {
      None
    }
  }

  def verifyTableAndSolidify(
                              tableDesc: CatalogTable,
                              query: Option[LogicalPlan],
                              maybeClusterBySpec: Option[ClusterBySpec] = None): CatalogTable = {
    if (tableDesc.bucketSpec.isDefined) {
      throw DeltaErrors.operationNotSupportedException("Bucketing", tableDesc.identifier)
    }

    val schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // Delta is case insensitive

    var validatedConfigurations =
      DeltaConfigs.validateConfigurations(tableDesc.properties)
    ClusteredTableUtils.validateExistingTableFeatureProperties(validatedConfigurations)
    // Add needed configs for Clustered table.
    if (maybeClusterBySpec.nonEmpty) {
      validatedConfigurations =
        validatedConfigurations ++
          ClusteredTableUtils.getClusteringColumnsAsProperty(maybeClusterBySpec) ++
          ClusteredTableUtils.getTableFeatureProperties(validatedConfigurations)
    }

    val db = tableDesc.identifier.database.getOrElse("default")
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = validatedConfigurations)
  }


  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasDeltaNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  private def hasDeltaNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head)
  }

  private def supportSQLOnFile: Boolean = SparkSession.active.sessionState.conf.runSQLonFile


  def validateClusterBySpec(
                             maybeClusterBySpec: Option[ClusterBySpec], schema: StructType): Unit = {
    maybeClusterBySpec.foreach { clusterBy =>
      // Check if the specified cluster by columns exists in the table.
      val resolver = SparkSession.active.sessionState.conf.resolver
      clusterBy.columnNames.foreach { column =>
        // This is the same check as in rules.scala, to keep the behaviour consistent.
        SchemaUtils.findColumnPosition(column.fieldNames(), schema, resolver)
      }
      // Check that columns are not duplicated in the cluster by statement.
      PartitionUtils.checkColumnNameDuplication(
        clusterBy.columnNames.map(_.toString), "in CLUSTER BY", resolver)
      // Check number of clustering columns is within allowed range.
      ClusteredTableUtils.validateNumClusteringColumns(
        clusterBy.columnNames.map(_.fieldNames.toSeq))
    }
  }

  private def convertTransforms(
                                 partitions: Seq[Transform]): (Seq[String], Option[BucketSpec], Option[ClusterBySpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]
    var clusterBySpec = Option.empty[ClusterBySpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, bucketCols, sortCols) =>
        bucketSpec = Some(BucketSpec(
          numBuckets, bucketCols.map(_.fieldNames.head), sortCols.map(_.fieldNames.head)))
      case TempClusterByTransform(columnNames) =>
        if (clusterBySpec.nonEmpty) {
          // Parser guarantees that it only passes down one TempClusterByTransform.
          throw SparkException.internalError("Cannot have multiple cluster by transforms.")
        }
        clusterBySpec = Some(ClusterBySpec(columnNames))

      case transform =>
        throw DeltaErrors.operationNotSupportedException(s"Partitioning by expressions")
    }
    // Parser guarantees that partition and cluster by can't both exist.
    assert(!(identityCols.toSeq.nonEmpty && clusterBySpec.nonEmpty))
    // Parser guarantees that bucketing and cluster by can't both exist.
    assert(!(bucketSpec.nonEmpty && clusterBySpec.nonEmpty))

    (identityCols.toSeq, bucketSpec, clusterBySpec)
  }

  def createDeltaTable(table: CatalogTable): Unit = {
    UnityCreateDeltaTableCommand(
      table,
      existingTableOpt = None,
      mode = TableCreationModes.Create.mode,
      query = None
    ).run(SparkSession.active)
    plugin.createTable(table, true)
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

  def loadTable(ident: Identifier, timestamp: Long): Table = {
    loadTableWithTimeTravel(ident, version = None, Some(timestamp))
  }

  def loadTable(ident: Identifier, version: String): Table = {
    loadTableWithTimeTravel(ident, Some(version), timestamp = None)
  }

  private def loadTableWithTimeTravel(
                                       ident: Identifier,
                                       version: Option[String],
                                       timestamp: Option[Long]): Table = {
    assert(version.isEmpty ^ timestamp.isEmpty,
      "Either the version or timestamp should be provided for time travel")
    val table = loadTable(ident)
    table match {
      case deltaTable: DeltaTableV2 =>
        val ttOpts = Map(DeltaDataSource.TIME_TRAVEL_SOURCE_KEY -> "SQL") ++
          (if (version.isDefined) {
            Map(DeltaDataSource.TIME_TRAVEL_VERSION_KEY -> version.get)
          } else {
            val timestampMs = timestamp.get / 1000
            Map(DeltaDataSource.TIME_TRAVEL_TIMESTAMP_KEY -> new Timestamp(timestampMs).toString)
          })

        deltaTable.withOptions(ttOpts)
      // punt this problem up to the parent
      case _ if version.isDefined => throw new IllegalArgumentException("version is only supported for delta tables")
      case _ if timestamp.isDefined => throw new IllegalArgumentException("timestamp is only supported for delta tables")
    }
  }


  def alterTable(ident: Identifier, changes: Seq[TableChange]): Table = {
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

  def stageCreate(
                   ident: Identifier,
                   schema: StructType,
                   partitions: Array[Transform],
                   properties: java.util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageCreate") {

      new StagedDeltaTableV2(
        ident,
        schema,
        partitions,
        properties,
        TableCreationModes.Create
      )
    }

  def stageReplace(
                    ident: Identifier,
                    schema: StructType,
                    partitions: Array[Transform],
                    properties: java.util.Map[String, String]): StagedTable =
    recordFrameProfile("DeltaCatalog", "stageReplace") {

      new StagedDeltaTableV2(
        ident,
        schema,
        partitions,
        properties,
        TableCreationModes.Replace
      )
    }

  def stageCreateOrReplace(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: java.util.Map[String, String]): StagedTable=
    recordFrameProfile("DeltaCatalog", "stageCreateOrReplace"){

    new StagedDeltaTableV2(
      ident,
      schema,
      partitions,
      properties,
      TableCreationModes.CreateOrReplace
    )
  }


  def getExpectedLocationOfTable(ident:TableIdentifier,  properties: java.util.Map[String, String]):(String, Boolean)={
    properties.asScala.get("location") match {
      case Some(res) => (res, true)
      case None =>
        val tablePath = (new Path(getDBPath(ident.database.getOrElse("default")).toString, ident.table)).toString
        (tablePath, false)
    }
  }

  def getDBPath(db: String): URI = {
    val warehousePath = SparkSession.active.sharedState.conf.get("spark.sql.warehouse.dir")
    val catalogPath = new Path(warehousePath, catalogName + ".cat")
    val dbPath = new Path(catalogPath, db + ".db")
    dbPath.toUri
  }

  private class StagedDeltaTableV2(
                                    ident: Identifier,
                                    override val schema: StructType,
                                    val partitions: Array[Transform],
                                    override val properties: java.util.Map[String, String],
                                    operation: TableCreationModes.CreationMode
                                  ) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = Map.empty

    override def partitioning(): Array[Transform] = partitions

    override def commitStagedChanges(): Unit = recordFrameProfile(
      "DeltaCatalog", "commitStagedChanges") {
      val conf = SparkSession.active.sessionState.conf
      val props = new java.util.HashMap[String, String]()
      // Options passed in through the SQL API will show up both with an "option." prefix and
      // without in Spark 3.1, so we need to remove those from the properties
      val optionsThroughProperties = properties.asScala.collect {
        case (k, _) if k.startsWith("option.") => k.stripPrefix("option.")
      }.toSet
      val sqlWriteOptions = new java.util.HashMap[String, String]()
      properties.asScala.foreach { case (k, v) =>
        if (!k.startsWith("option.") && !optionsThroughProperties.contains(k)) {
          // Do not add to properties
          props.put(k, v)
        } else if (optionsThroughProperties.contains(k)) {
          sqlWriteOptions.put(k, v)
        }
      }
      if (writeOptions.isEmpty && !sqlWriteOptions.isEmpty) {
        writeOptions = sqlWriteOptions.asScala.toMap
      }
      if (SparkSession.active.sessionState.conf.getConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS)) {
        // Legacy behavior
        writeOptions.foreach { case (k, v) => props.put(k, v) }
      } else {
        writeOptions.foreach { case (k, v) =>
          // Continue putting in Delta prefixed options to avoid breaking workloads
          if (k.toLowerCase(Locale.ROOT).startsWith("delta.")) {
            props.put(k, v)
          }
        }
      }
      val id = {
        TableIdentifier(ident.name(), ident.namespace().lastOption)
      }
      val oct = getExistingTableIfExists(id)
      val (locString, isExternal) = oct match {
        case Some(ct) =>  (ct.storage.locationUri.get.toString, ct.tableType == CatalogTableType.EXTERNAL)
        case None => getExpectedLocationOfTable(id, properties)
      }


      createDeltaTable(
        ident,
        schema,
        partitions,
        props,
        writeOptions,
        asSelectQuery,
        operation,
        locString,
        isExternal
      )
    }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): java.util.Set[TableCapability] = {
      Set(V1_BATCH_WRITE).asJava
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      writeOptions = info.options.asCaseSensitiveMap().asScala.toMap
      new DeltaV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a Delta table.
     */
    private class DeltaV1WriteBuilder extends WriteBuilder {
      override def build(): V1Write = new V1Write {
        override def toInsertableRelation(): InsertableRelation = {
          new InsertableRelation {
            override def insert(data: DataFrame, overwrite: Boolean): Unit = {
              asSelectQuery = Option(data)
            }
          }
        }
      }
    }
  }

}

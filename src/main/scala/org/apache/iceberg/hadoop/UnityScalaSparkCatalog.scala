//package org.apache.iceberg.hadoop
//
//import java.util.{Arrays, List, Map, Set, TreeMap}
//
//
//
//import org.apache.iceberg.TableProperties.GC_ENABLED
//import org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT
//import java.util
//import java.util.Objects
//import java.util.concurrent.TimeUnit
//import java.util.regex.Matcher
//import java.util.regex.Pattern
//import java.util.stream.Stream
//import org.apache.hadoop.conf.Configuration
//import org.apache.iceberg._
//import org.apache.iceberg.catalog.Catalog
//import org.apache.iceberg.catalog.Namespace
//import org.apache.iceberg.catalog.SupportsNamespaces
//import org.apache.iceberg.catalog.TableIdentifier
//import org.apache.iceberg.catalog.ViewCatalog
//import org.apache.iceberg.exceptions.AlreadyExistsException
//import org.apache.iceberg.exceptions.CommitFailedException
//import org.apache.iceberg.exceptions.ValidationException
//import org.apache.iceberg.hadoop.HadoopCatalog
//import org.apache.iceberg.hadoop.HadoopTables
//import org.apache.iceberg.relocated.com.google.common.base.Joiner
//import org.apache.iceberg.relocated.com.google.common.base.Preconditions
//import org.apache.iceberg.relocated.com.google.common.base.Splitter
//import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList
//import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
//import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet
//import org.apache.iceberg.relocated.com.google.common.collect.Lists
//import org.apache.iceberg.relocated.com.google.common.collect.Maps
//import org.apache.iceberg.relocated.com.google.common.collect.Sets
//import org.apache.iceberg.spark._
//import org.apache.iceberg.spark.actions.SparkActions
//import org.apache.iceberg.spark.source.SparkChangelogTable
//import org.apache.iceberg.spark.source.SparkTable
//import org.apache.iceberg.spark.source.SparkView
//import org.apache.iceberg.spark.source.StagedSparkTable
//import org.apache.iceberg.util.Pair
//import org.apache.iceberg.util.PropertyUtil
//import org.apache.iceberg.util.SnapshotUtil
//import org.apache.iceberg.view.UpdateViewProperties
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException
//import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
//import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
//import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
//import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
//import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
//import org.apache.spark.sql.connector.catalog.Identifier
//import org.apache.spark.sql.connector.catalog.NamespaceChange
//import org.apache.spark.sql.connector.catalog.StagedTable
//import org.apache.spark.sql.connector.catalog.Table
//import org.apache.spark.sql.connector.catalog.TableCatalog
//import org.apache.spark.sql.connector.catalog.TableChange
//import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange
//import org.apache.spark.sql.connector.catalog.TableChange.RemoveProperty
//import org.apache.spark.sql.connector.catalog.TableChange.SetProperty
//import org.apache.spark.sql.connector.catalog.View
//import org.apache.spark.sql.connector.catalog.ViewChange
//import org.apache.spark.sql.connector.expressions.Transform
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.util.CaseInsensitiveStringMap
//
//
//
//object UnityScalaSparkCatalog {
//  private val DEFAULT_NS_KEYS = ImmutableSet.of(TableCatalog.PROP_OWNER)
//  private val COMMA = Splitter.on(",")
//  private val COMMA_JOINER = Joiner.on(",")
//  private val AT_TIMESTAMP = Pattern.compile("at_timestamp_(\\d+)")
//  private val SNAPSHOT_ID = Pattern.compile("snapshot_id_(\\d+)")
//  private val BRANCH = Pattern.compile("branch_(.*)")
//  private val TAG = Pattern.compile("tag_(.*)")
//
//  private def verifyNonReservedProperty(property: String, errorMsg: String): Unit = {
//    if (SparkView.RESERVED_PROPERTIES.contains(property)) throw new UnsupportedOperationException(String.format(errorMsg, property))
//  }
//
//  private def verifyNonReservedPropertyIsUnset(property: String): Unit = {
//    verifyNonReservedProperty(property, "Cannot unset reserved property: '%s'")
//  }
//
//  private def verifyNonReservedPropertyIsSet(property: String): Unit = {
//    verifyNonReservedProperty(property, "Cannot set reserved property: '%s'")
//  }
//
//  private def commitChanges(table: Table, setLocation: TableChange.SetProperty, setSnapshotId: TableChange.SetProperty, pickSnapshotId: TableChange.SetProperty, propertyChanges: util.List[TableChange], schemaChanges: util.List[TableChange]): Unit = {
//    // don't allow setting the snapshot and picking a commit at the same time because order is
//    // ambiguous and choosing one order leads to different results
//    Preconditions.checkArgument(setSnapshotId == null || pickSnapshotId == null, "Cannot set the current the current snapshot ID and cherry-pick snapshot changes")
//    if (setSnapshotId != null) {
//      val newSnapshotId = Long.parseLong(setSnapshotId.value)
//      table.manageSnapshots.setCurrentSnapshot(newSnapshotId).commit()
//    }
//    // if updating the table snapshot, perform that update first in case it fails
//    if (pickSnapshotId != null) {
//      val newSnapshotId = Long.parseLong(pickSnapshotId.value)
//      table.manageSnapshots.cherrypick(newSnapshotId).commit()
//    }
//    val transaction = table.newTransaction
//    if (setLocation != null) transaction.updateLocation.setLocation(setLocation.value).commit()
//    if (!propertyChanges.isEmpty) Spark3Util.applyPropertyChanges(transaction.updateProperties, propertyChanges).commit()
//    if (!schemaChanges.isEmpty) Spark3Util.applySchemaChanges(transaction.updateSchema, schemaChanges).commit()
//    transaction.commitTransaction()
//  }
//
//  private def isPathIdentifier(ident: Identifier) = ident.isInstanceOf[PathIdentifier]
//
//  private def checkNotPathIdentifier(identifier: Identifier, method: String): Unit = {
//    if (identifier.isInstanceOf[PathIdentifier]) throw new IllegalArgumentException(String.format("Cannot pass path based identifier to %s method. %s is a path.", method, identifier))
//  }
//}
//
//class UnityScalaSparkCatalog extends ViewCatalog with SupportsReplaceView {
//  private var catalogName: String = null
//  private var icebergCatalog: Catalog = null
//  private var cacheEnabled = CatalogProperties.CACHE_ENABLED_DEFAULT
//  private var asNamespaceCatalog: SupportsNamespaces = null
//  private var asViewCatalog: ViewCatalog = null
//  private var defaultNamespace: Array[String] = null
//  private var tables: HadoopTables = null
//
//  /**
//   * Build an Iceberg {@link Catalog} to be used by this Spark catalog adapter.
//   *
//   * @param name    Spark's catalog name
//   * @param options Spark's catalog options
//   * @return an Iceberg catalog
//   */
//  protected def buildIcebergCatalog(name: String, options: CaseInsensitiveStringMap): Catalog = {
//    val conf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active, name)
//    val optionsMap = new util.TreeMap[String, String](String.CASE_INSENSITIVE_ORDER)
//    optionsMap.putAll(options.asCaseSensitiveMap)
//    optionsMap.put(CatalogProperties.APP_ID, SparkSession.active.sparkContext.applicationId)
//    optionsMap.put(CatalogProperties.USER, SparkSession.active.sparkContext.sparkUser)
//    val catalog = new UnityHadoopCatalog
//    catalog.initialize(name, options)
//    catalog
//  }
//
//  /**
//   * Build an Iceberg {@link TableIdentifier} for the given Spark identifier.
//   *
//   * @param identifier Spark's identifier
//   * @return an Iceberg identifier
//   */
//  protected def buildIdentifier(identifier: Identifier): TableIdentifier = Spark3Util.identifierToTableIdentifier(identifier)
//
//  @throws[NoSuchTableException]
//  def loadTable(ident: Identifier): Table = try load(ident)
//  catch {
//    case e: NoSuchTableException =>
//      throw e
//  }
//
//  @throws[NoSuchTableException]
//  def loadTable(ident: Identifier, version: String): Table = {
//    val table = loadTable(ident)
//    if (table.isInstanceOf[SparkTable]) {
//      val sparkTable = table.asInstanceOf[SparkTable]
//      Preconditions.checkArgument(sparkTable.snapshotId == null && sparkTable.branch == null, "Cannot do time-travel based on both table identifier and AS OF")
//      try sparkTable.copyWithSnapshotId(Long.parseLong(version))
//      catch {
//        case e: NumberFormatException =>
//          val ref = sparkTable.table.refs.get(version)
//          ValidationException.check(ref != null, "Cannot find matching snapshot ID or reference name for version " + version)
//          if (ref.isBranch) sparkTable.copyWithBranch(version)
//          else sparkTable.copyWithSnapshotId(ref.snapshotId)
//      }
//    }
//    else if (table.isInstanceOf[SparkChangelogTable]) throw new UnsupportedOperationException("AS OF is not supported for changelogs")
//    else throw new IllegalArgumentException("Unknown Spark table type: " + table.getClass.getName)
//  }
//
//  @throws[NoSuchTableException]
//  def loadTable(ident: Identifier, timestamp: Long): Table = {
//    val table = loadTable(ident)
//    if (table.isInstanceOf[SparkTable]) {
//      val sparkTable = table.asInstanceOf[SparkTable]
//      Preconditions.checkArgument(sparkTable.snapshotId == null, "Cannot do time-travel based on both table identifier and AS OF")
//      // convert the timestamp to milliseconds as Spark passes microseconds
//      // but Iceberg uses milliseconds for snapshot timestamps
//      val timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp)
//      val snapshotId = SnapshotUtil.snapshotIdAsOfTime(sparkTable.table, timestampMillis)
//      sparkTable.copyWithSnapshotId(snapshotId)
//    }
//    else if (table.isInstanceOf[SparkChangelogTable]) throw new UnsupportedOperationException("AS OF is not supported for changelogs")
//    else throw new IllegalArgumentException("Unknown Spark table type: " + table.getClass.getName)
//  }
//
//  @throws[TableAlreadyExistsException]
//  def createTable(ident: Identifier, schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): Table = {
//    val icebergSchema = SparkSchemaUtil.convert(schema)
//    try {
//      val builder = newBuilder(ident, icebergSchema)
//      val icebergTable = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms)).withLocation(properties.get("location")).withProperties(Spark3Util.rebuildCreateProperties(properties)).create
//      new SparkTable(icebergTable, !cacheEnabled)
//    } catch {
//      case e: AlreadyExistsException =>
//        throw new TableAlreadyExistsException(ident)
//    }
//  }
//
//  @throws[TableAlreadyExistsException]
//  def stageCreate(ident: Identifier, schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): StagedTable = {
//    val icebergSchema = SparkSchemaUtil.convert(schema)
//    try {
//      val builder = newBuilder(ident, icebergSchema)
//      val transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms)).withLocation(properties.get("location")).withProperties(Spark3Util.rebuildCreateProperties(properties)).createTransaction
//      new StagedSparkTable(transaction)
//    } catch {
//      case e: AlreadyExistsException =>
//        throw new TableAlreadyExistsException(ident)
//    }
//  }
//
//  @throws[NoSuchTableException]
//  def stageReplace(ident: Identifier, schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): StagedTable = {
//    val icebergSchema = SparkSchemaUtil.convert(schema)
//    try {
//      val builder = newBuilder(ident, icebergSchema)
//      val transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms)).withLocation(properties.get("location")).withProperties(Spark3Util.rebuildCreateProperties(properties)).replaceTransaction
//      new StagedSparkTable(transaction)
//    } catch {
//      case e: NoSuchTableException =>
//        throw new NoSuchTableException(ident)
//    }
//  }
//
//  def stageCreateOrReplace(ident: Identifier, schema: StructType, transforms: Array[Transform], properties: util.Map[String, String]): StagedTable = {
//    val icebergSchema = SparkSchemaUtil.convert(schema)
//    val builder = newBuilder(ident, icebergSchema)
//    val transaction = builder.withPartitionSpec(Spark3Util.toPartitionSpec(icebergSchema, transforms)).withLocation(properties.get("location")).withProperties(Spark3Util.rebuildCreateProperties(properties)).createOrReplaceTransaction
//    new StagedSparkTable(transaction)
//  }
//
//  @throws[NoSuchTableException]
//  def alterTable(ident: Identifier, changes: TableChange*): Table = {
//    var setLocation: TableChange.SetProperty = null
//    var setSnapshotId: TableChange.SetProperty = null
//    var pickSnapshotId: TableChange.SetProperty = null
//    val propertyChanges = Lists.newArrayList
//    val schemaChanges = Lists.newArrayList
//    for (change <- changes) {
//      if (change.isInstanceOf[TableChange.SetProperty]) {
//        val set = change.asInstanceOf[TableChange.SetProperty]
//        if (TableCatalog.PROP_LOCATION.equalsIgnoreCase(set.property)) setLocation = set
//        else if ("current-snapshot-id".equalsIgnoreCase(set.property)) setSnapshotId = set
//        else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.property)) pickSnapshotId = set
//        else if ("sort-order".equalsIgnoreCase(set.property)) throw new UnsupportedOperationException("Cannot specify the 'sort-order' because it's a reserved table " + "property. Please use the command 'ALTER TABLE ... WRITE ORDERED BY' to specify write sort-orders.")
//        else if ("identifier-fields".equalsIgnoreCase(set.property)) throw new UnsupportedOperationException("Cannot specify the 'identifier-fields' because it's a reserved table property. " + "Please use the command 'ALTER TABLE ... SET IDENTIFIER FIELDS' to specify identifier fields.")
//        else propertyChanges.add(set)
//      }
//      else if (change.isInstanceOf[TableChange.RemoveProperty]) propertyChanges.add(change)
//      else if (change.isInstanceOf[TableChange.ColumnChange]) schemaChanges.add(change)
//      else throw new UnsupportedOperationException("Cannot apply unknown table change: " + change)
//    }
//    try {
//      val table = icebergCatalog.loadTable(buildIdentifier(ident))
//      UnitySparkCatalog.commitChanges(table, setLocation, setSnapshotId, pickSnapshotId, propertyChanges, schemaChanges)
//      new SparkTable(table, true) /* refreshEagerly */
//    } catch {
//      case e: NoSuchTableException =>
//        throw new NoSuchTableException(ident)
//    }
//  }
//
//  def dropTable(ident: Identifier): Boolean = dropTableWithoutPurging(ident)
//
//  def purgeTable(ident: Identifier): Boolean = try {
//    val table = icebergCatalog.loadTable(buildIdentifier(ident))
//    ValidationException.check(PropertyUtil.propertyAsBoolean(table.properties, GC_ENABLED, GC_ENABLED_DEFAULT), "Cannot purge table: GC is disabled (deleting files may corrupt other tables)")
//    val metadataFileLocation = table.asInstanceOf[HasTableOperations].operations.current.metadataFileLocation
//    val dropped = dropTableWithoutPurging(ident)
//    if (dropped) {
//      // check whether the metadata file exists because HadoopCatalog/HadoopTables
//      // will drop the warehouse directly and ignore the `purge` argument
//      val metadataFileExists = table.io.newInputFile(metadataFileLocation).exists
//      if (metadataFileExists) SparkActions.get.deleteReachableFiles(metadataFileLocation).io(table.io).execute
//    }
//    dropped
//  } catch {
//    case e: NoSuchTableException =>
//      false
//  }
//
//  private def dropTableWithoutPurging(ident: Identifier) = if (UnitySparkCatalog.isPathIdentifier(ident)) tables.dropTable(ident.asInstanceOf[PathIdentifier].location, false /* don't purge data */)
//  else icebergCatalog.dropTable(buildIdentifier(ident), false /* don't purge data */)
//
//  @throws[NoSuchTableException]
//  @throws[TableAlreadyExistsException]
//  def renameTable(from: Identifier, to: Identifier): Unit = {
//    try {
//      UnitySparkCatalog.checkNotPathIdentifier(from, "renameTable")
//      UnitySparkCatalog.checkNotPathIdentifier(to, "renameTable")
//      icebergCatalog.renameTable(buildIdentifier(from), buildIdentifier(to))
//    } catch {
//      case e: NoSuchTableException =>
//        throw new NoSuchTableException(from)
//      case e: AlreadyExistsException =>
//        throw new TableAlreadyExistsException(to)
//    }
//  }
//
//  def invalidateTable(ident: Identifier): Unit = {
//    if (!UnitySparkCatalog.isPathIdentifier(ident)) icebergCatalog.invalidateTable(buildIdentifier(ident))
//  }
//
//  def listTables(namespace: Array[String]): Array[Identifier] = icebergCatalog.listTables(Namespace.of(namespace)).stream.map((ident: TableIdentifier) => Identifier.of(ident.namespace.levels, ident.name)).toArray(`new`)
//
//  override def defaultNamespace: Array[String] = {
//    if (defaultNamespace != null) return defaultNamespace
//    new Array[String](0)
//  }
//
//  def listNamespaces: Array[Array[String]] = {
//    if (asNamespaceCatalog != null) return asNamespaceCatalog.listNamespaces.stream.map(Namespace.levels).toArray(`new`)
//    new Array[Array[String]](0)
//  }
//
//  @throws[NoSuchNamespaceException]
//  def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
//    if (asNamespaceCatalog != null) try return asNamespaceCatalog.listNamespaces(Namespace.of(namespace)).stream.map(Namespace.levels).toArray(`new`)
//    catch {
//      case e: NoSuchNamespaceException =>
//        throw new NoSuchNamespaceException(namespace)
//    }
//    throw new NoSuchNamespaceException(namespace)
//  }
//
//  @throws[NoSuchNamespaceException]
//  def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
//    if (asNamespaceCatalog != null) try return asNamespaceCatalog.loadNamespaceMetadata(Namespace.of(namespace))
//    catch {
//      case e: NoSuchNamespaceException =>
//        throw new NoSuchNamespaceException(namespace)
//    }
//    throw new NoSuchNamespaceException(namespace)
//  }
//
//  @throws[NamespaceAlreadyExistsException]
//  def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
//    if (asNamespaceCatalog != null) try if (asNamespaceCatalog.isInstanceOf[HadoopCatalog] && UnitySparkCatalog.DEFAULT_NS_KEYS == metadata.keySet) {
//      // Hadoop catalog will reject metadata properties, but Spark automatically adds "owner".
//      // If only the automatic properties are present, replace metadata with an empty map.
//      asNamespaceCatalog.createNamespace(Namespace.of(namespace), ImmutableMap.of)
//    }
//    else asNamespaceCatalog.createNamespace(Namespace.of(namespace), metadata)
//    catch {
//      case e: AlreadyExistsException =>
//        throw new NamespaceAlreadyExistsException(namespace)
//    }
//    else throw new UnsupportedOperationException("Namespaces are not supported by catalog: " + catalogName)
//  }
//
//  @throws[NoSuchNamespaceException]
//  def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
//    if (asNamespaceCatalog != null) {
//      val updates = Maps.newHashMap
//      val removals = Sets.newHashSet
//      for (change <- changes) {
//        if (change.isInstanceOf[NamespaceChange.SetProperty]) {
//          val set = change.asInstanceOf[NamespaceChange.SetProperty]
//          updates.put(set.property, set.value)
//        }
//        else if (change.isInstanceOf[NamespaceChange.RemoveProperty]) removals.add(change.asInstanceOf[NamespaceChange.RemoveProperty].property)
//        else throw new UnsupportedOperationException("Cannot apply unknown namespace change: " + change)
//      }
//      try {
//        if (!updates.isEmpty) asNamespaceCatalog.setProperties(Namespace.of(namespace), updates)
//        if (!removals.isEmpty) asNamespaceCatalog.removeProperties(Namespace.of(namespace), removals)
//      } catch {
//        case e: NoSuchNamespaceException =>
//          throw new NoSuchNamespaceException(namespace)
//      }
//    }
//    else throw new NoSuchNamespaceException(namespace)
//  }
//
//  @throws[NoSuchNamespaceException]
//  def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
//    if (asNamespaceCatalog != null) try return asNamespaceCatalog.dropNamespace(Namespace.of(namespace))
//    catch {
//      case e: NoSuchNamespaceException =>
//        throw new NoSuchNamespaceException(namespace)
//    }
//    false
//  }
//
//  override def listViews(namespace: String*): Array[Identifier] = {
//    if (null != asViewCatalog) return asViewCatalog.listViews(Namespace.of(namespace)).stream.map((ident: TableIdentifier) => Identifier.of(ident.namespace.levels, ident.name)).toArray(`new`)
//    new Array[Identifier](0)
//  }
//
//  @throws[NoSuchViewException]
//  override def loadView(ident: Identifier): View = {
//    if (null != asViewCatalog) try {
//      val view = asViewCatalog.loadView(buildIdentifier(ident))
//      return new SparkView(catalogName, view)
//    } catch {
//      case e: NoSuchViewException =>
//        throw new NoSuchViewException(ident)
//    }
//    throw new NoSuchViewException(ident)
//  }
//
//  @throws[ViewAlreadyExistsException]
//  @throws[NoSuchNamespaceException]
//  override def createView(ident: Identifier, sql: String, currentCatalog: String, currentNamespace: Array[String], schema: StructType, queryColumnNames: Array[String], columnAliases: Array[String], columnComments: Array[String], properties: util.Map[String, String]): View = {
//    if (null != asViewCatalog) {
//      val icebergSchema = SparkSchemaUtil.convert(schema)
//      try {
//        val props = ImmutableMap.builder[String, String].putAll(Spark3Util.rebuildCreateProperties(properties)).put(SparkView.QUERY_COLUMN_NAMES, UnitySparkCatalog.COMMA_JOINER.join(queryColumnNames)).buildKeepingLast
//        val view = asViewCatalog.buildView(buildIdentifier(ident)).withDefaultCatalog(currentCatalog).withDefaultNamespace(Namespace.of(currentNamespace)).withQuery("spark", sql).withSchema(icebergSchema).withLocation(properties.get("location")).withProperties(props).create
//        return new SparkView(catalogName, view)
//      } catch {
//        case e: NoSuchNamespaceException =>
//          throw new NoSuchNamespaceException(currentNamespace)
//        case e: AlreadyExistsException =>
//          throw new ViewAlreadyExistsException(ident)
//      }
//    }
//    throw new UnsupportedOperationException("Creating a view is not supported by catalog: " + catalogName)
//  }
//
//  @throws[NoSuchNamespaceException]
//  @throws[NoSuchViewException]
//  override def replaceView(ident: Identifier, sql: String, currentCatalog: String, currentNamespace: Array[String], schema: StructType, queryColumnNames: Array[String], columnAliases: Array[String], columnComments: Array[String], properties: util.Map[String, String]): View = {
//    if (null != asViewCatalog) {
//      val icebergSchema = SparkSchemaUtil.convert(schema)
//      try {
//        val props = ImmutableMap.builder[String, String].putAll(Spark3Util.rebuildCreateProperties(properties)).put(SparkView.QUERY_COLUMN_NAMES, UnitySparkCatalog.COMMA_JOINER.join(queryColumnNames)).buildKeepingLast
//        val view = asViewCatalog.buildView(buildIdentifier(ident)).withDefaultCatalog(currentCatalog).withDefaultNamespace(Namespace.of(currentNamespace)).withQuery("spark", sql).withSchema(icebergSchema).withLocation(properties.get("location")).withProperties(props).createOrReplace
//        return new SparkView(catalogName, view)
//      } catch {
//        case e: NoSuchNamespaceException =>
//          throw new NoSuchNamespaceException(currentNamespace)
//        case e: NoSuchViewException =>
//          throw new NoSuchViewException(ident)
//      }
//    }
//    throw new UnsupportedOperationException("Replacing a view is not supported by catalog: " + catalogName)
//  }
//
//  @throws[NoSuchViewException]
//  @throws[IllegalArgumentException]
//  override def alterView(ident: Identifier, changes: ViewChange*): View = {
//    if (null != asViewCatalog) try {
//      val view = asViewCatalog.loadView(buildIdentifier(ident))
//      val updateViewProperties = view.updateProperties
//      for (change <- changes) {
//        if (change.isInstanceOf[ViewChange.SetProperty]) {
//          val property = change.asInstanceOf[ViewChange.SetProperty]
//          UnitySparkCatalog.verifyNonReservedPropertyIsSet(property.property)
//          updateViewProperties.set(property.property, property.value)
//        }
//        else if (change.isInstanceOf[ViewChange.RemoveProperty]) {
//          val remove = change.asInstanceOf[ViewChange.RemoveProperty]
//          UnitySparkCatalog.verifyNonReservedPropertyIsUnset(remove.property)
//          updateViewProperties.remove(remove.property)
//        }
//      }
//      updateViewProperties.commit()
//      return new SparkView(catalogName, view)
//    } catch {
//      case e: NoSuchViewException =>
//        throw new NoSuchViewException(ident)
//    }
//    throw new UnsupportedOperationException("Altering a view is not supported by catalog: " + catalogName)
//  }
//
//  override def dropView(ident: Identifier): Boolean = {
//    if (null != asViewCatalog) return asViewCatalog.dropView(buildIdentifier(ident))
//    false
//  }
//
//  @throws[NoSuchViewException]
//  @throws[ViewAlreadyExistsException]
//  override def renameView(fromIdentifier: Identifier, toIdentifier: Identifier): Unit = {
//    if (null != asViewCatalog) try asViewCatalog.renameView(buildIdentifier(fromIdentifier), buildIdentifier(toIdentifier))
//    catch {
//      case e: NoSuchViewException =>
//        throw new NoSuchViewException(fromIdentifier)
//      case e: AlreadyExistsException =>
//        throw new ViewAlreadyExistsException(toIdentifier)
//    }
//    else throw new UnsupportedOperationException("Renaming a view is not supported by catalog: " + catalogName)
//  }
//
//  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
//    this.cacheEnabled = PropertyUtil.propertyAsBoolean(options, CatalogProperties.CACHE_ENABLED, CatalogProperties.CACHE_ENABLED_DEFAULT)
//    val cacheCaseSensitive = PropertyUtil.propertyAsBoolean(options, CatalogProperties.CACHE_CASE_SENSITIVE, CatalogProperties.CACHE_CASE_SENSITIVE_DEFAULT)
//    val cacheExpirationIntervalMs = PropertyUtil.propertyAsLong(options, CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS, CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
//    // An expiration interval of 0ms effectively disables caching.
//    // Do not wrap with CachingCatalog.
//    if (cacheExpirationIntervalMs == 0) this.cacheEnabled = false
//    val catalog = buildIcebergCatalog(name, options)
//    this.catalogName = name
//    val sparkSession = SparkSession.active
//    this.tables = new HadoopTables(SparkUtil.hadoopConfCatalogOverrides(SparkSession.active, name))
//    this.icebergCatalog = catalog
//    //        this.icebergCatalog =
//    //                cacheEnabled
//    //                        ? CachingCatalog.wrap(catalog, cacheCaseSensitive, cacheExpirationIntervalMs)
//    //                        : catalog;
//    if (catalog.isInstanceOf[SupportsNamespaces]) {
//      this.asNamespaceCatalog = catalog.asInstanceOf[SupportsNamespaces]
//      if (options.containsKey("default-namespace")) this.defaultNamespace = Splitter.on('.').splitToList(options.get("default-namespace")).toArray(new Array[String](0))
//    }
//    if (catalog.isInstanceOf[ViewCatalog]) this.asViewCatalog = catalog.asInstanceOf[ViewCatalog]
//    EnvironmentContext.put(EnvironmentContext.ENGINE_NAME, "spark")
//    EnvironmentContext.put(EnvironmentContext.ENGINE_VERSION, sparkSession.sparkContext.version)
//    EnvironmentContext.put(CatalogProperties.APP_ID, sparkSession.sparkContext.applicationId)
//  }
//
//  override def name: String = catalogName
//
//  private def load(ident: Identifier): Table = {
//    if (UnityScalaSparkCatalog.isPathIdentifier(ident)) return loadFromPathIdentifier(ident.asInstanceOf[PathIdentifier])
//    try {
//      val table = icebergCatalog.loadTable(buildIdentifier(ident))
//      new SparkTable(table, !cacheEnabled)
//    } catch {
//      case e: NoSuchTableException =>
//        if (ident.namespace.length == 0) throw e
//        // if the original load didn't work, try using the namespace as an identifier because
//        // the original identifier may include a snapshot selector or may point to the changelog
//        val namespaceAsIdent = buildIdentifier(namespaceToIdentifier(ident.namespace))
//        var table: Table = null
//        try table = icebergCatalog.loadTable(namespaceAsIdent)
//        catch {
//          case ignored: Exception =>
//
//            // the namespace does not identify a table, so it cannot be a table with a snapshot selector
//            // throw the original exception
//            throw e
//        }
//        // loading the namespace as a table worked, check the name to see if it is a valid selector
//        // or if the name points to the changelog
//        if (ident.name.equalsIgnoreCase(SparkChangelogTable.TABLE_NAME)) return new SparkChangelogTable(table, !cacheEnabled)
//        val at = UnitySparkCatalog.AT_TIMESTAMP.matcher(ident.name)
//        if (at.matches) {
//          val asOfTimestamp = Long.parseLong(at.group(1))
//          val snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, asOfTimestamp)
//          return new SparkTable(table, snapshotId, !cacheEnabled)
//        }
//        val id = UnitySparkCatalog.SNAPSHOT_ID.matcher(ident.name)
//        if (id.matches) {
//          val snapshotId = Long.parseLong(id.group(1))
//          return new SparkTable(table, snapshotId, !cacheEnabled)
//        }
//        val branch = UnitySparkCatalog.BRANCH.matcher(ident.name)
//        if (branch.matches) return new SparkTable(table, branch.group(1), !cacheEnabled)
//        val tag = UnitySparkCatalog.TAG.matcher(ident.name)
//        if (tag.matches) {
//          val tagSnapshot = table.snapshot(tag.group(1))
//          if (tagSnapshot != null) return new SparkTable(table, tagSnapshot.snapshotId, !cacheEnabled)
//        }
//        // the name wasn't a valid snapshot selector and did not point to the changelog
//        // throw the original exception
//        throw e
//    }
//  }
//
//  private def parseLocationString(location: String) = {
//    val hashIndex = location.lastIndexOf('#')
//    if (hashIndex != -1 && !location.endsWith("#")) {
//      val baseLocation = location.substring(0, hashIndex)
//      val metadata = UnitySparkCatalog.COMMA.splitToList(location.substring(hashIndex + 1))
//      Pair.of(baseLocation, metadata)
//    }
//    else Pair.of(location, ImmutableList.of)
//  }
//
//  @SuppressWarnings(Array("CyclomaticComplexity")) private def loadFromPathIdentifier(ident: PathIdentifier) = {
//    val parsed = parseLocationString(ident.location)
//    var metadataTableName: String = null
//    var asOfTimestamp: Long = null
//    var snapshotId: Long = null
//    var branch: String = null
//    var tag: String = null
//    var isChangelog = false
//    import scala.collection.JavaConversions._
//    for (meta <- parsed.second) {
//      if (meta.equalsIgnoreCase(SparkChangelogTable.TABLE_NAME)) {
//        isChangelog = true
//        continue //todo: continue is not supported
//
//      }
//      if (MetadataTableType.from(meta) != null) {
//        metadataTableName = meta
//        continue //todo: continue is not supported
//
//      }
//      val at = UnitySparkCatalog.AT_TIMESTAMP.matcher(meta)
//      if (at.matches) {
//        asOfTimestamp = Long.parseLong(at.group(1))
//        continue //todo: continue is not supported
//
//      }
//      val id = UnitySparkCatalog.SNAPSHOT_ID.matcher(meta)
//      if (id.matches) {
//        snapshotId = Long.parseLong(id.group(1))
//        continue //todo: continue is not supported
//
//      }
//      val branchRef = UnitySparkCatalog.BRANCH.matcher(meta)
//      if (branchRef.matches) {
//        branch = branchRef.group(1)
//        continue //todo: continue is not supported
//
//      }
//      val tagRef = UnitySparkCatalog.TAG.matcher(meta)
//      if (tagRef.matches) tag = tagRef.group(1)
//    }
//    Preconditions.checkArgument(Stream.of(snapshotId, asOfTimestamp, branch, tag).filter(Objects.nonNull).count <= 1, "Can specify only one of snapshot-id (%s), as-of-timestamp (%s), branch (%s), tag (%s)", snapshotId, asOfTimestamp, branch, tag)
//    Preconditions.checkArgument(!isChangelog || (snapshotId == null && asOfTimestamp == null), "Cannot specify snapshot-id and as-of-timestamp for changelogs")
//    val table = tables.load(parsed.first + (if (metadataTableName != null) "#" + metadataTableName
//    else ""))
//    if (isChangelog) new SparkChangelogTable(table, !cacheEnabled)
//    else if (asOfTimestamp != null) {
//      val snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table, asOfTimestamp)
//      new SparkTable(table, snapshotIdAsOfTime, !cacheEnabled)
//    }
//    else if (branch != null) new SparkTable(table, branch, !cacheEnabled)
//    else if (tag != null) {
//      val tagSnapshot = table.snapshot(tag)
//      Preconditions.checkArgument(tagSnapshot != null, "Cannot find snapshot associated with tag name: %s", tag)
//      new SparkTable(table, tagSnapshot.snapshotId, !cacheEnabled)
//    }
//    else new SparkTable(table, snapshotId, !cacheEnabled)
//  }
//
//  private def namespaceToIdentifier(namespace: Array[String]) = {
//    Preconditions.checkArgument(namespace.length > 0, "Cannot convert empty namespace to identifier")
//    val ns = util.Arrays.copyOf(namespace, namespace.length - 1)
//    val name = namespace(ns.length)
//    Identifier.of(ns, name)
//  }
//
//  private def newBuilder(ident: Identifier, schema: Schema) = if (UnityScalaSparkCatalog.isPathIdentifier(ident)) tables.buildTable(ident.asInstanceOf[PathIdentifier].location, schema)
//  else icebergCatalog.buildTable(buildIdentifier(ident), schema)
//
//
//}
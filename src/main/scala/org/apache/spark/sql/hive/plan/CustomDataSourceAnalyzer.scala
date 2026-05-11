package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.arrow.ArrowFileFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.catalyst.{AliasIdentifier, QueryPlanningTracker, TableIdentifier, parser}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, EliminateSubqueryAliases, GetColumnByOrdinal, GetViewColumnByNameAndOrdinal, NamedRelation, RelationTimeTravel, ResolveInlineTables, ResolvedIdentifier, ResolvedTable, UnresolvedAttribute, UnresolvedFunction, UnresolvedInlineTable, UnresolvedLeafNode, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, NamedExpression, SubqueryExpression, UpCast}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, DeltaDelete, DeltaMergeInto, DeltaUpdateTable, DescribeRelation, DeserializeToObject, Filter, InsertIntoStatement, LocalRelation, LogicalPlan, OverwriteByExpression, Project, ReplaceTableAsSelect, SerdeInfo, SubqueryAlias, TableSpec, TableSpecBase, View}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, TreeNodeTag}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.{convertTableProperties, withDefaultOwnership}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, CatalogV2Util, Identifier, StagedTable, Table, TableCatalog, TableSchemaChangeCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.{DeltaAnalysis, DeltaErrors, DeltaRelation, PreprocessTableMerge, PreprocessTableUpdate, ResolveDeltaPathTable, TableChanges}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeleteCommand
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.{CustomInsertIntoHadoopFsRelationCommand, DataSource, FileFormat, HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.hive.catalog.BestEffortStagedTable
import org.apache.spark.sql.hive.plan.listener.{CatalogQueryExecutionListener, ListenerUtil}
import org.apache.spark.sql.hive.plan.spark.sql.connector.{V2CustomTable, V2Table}
import org.apache.spark.sql.hive.plan.spark.sql.execution.CustomCatalogFileIndex
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.ViewUnresolvedRelation
import org.apache.spark.sql.hive.plan.spark.sql.parser.CustomSparkSQLParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.plan.spark.sql.execution.plan.CustomCreateDataSourceTableAsSelectCommand

import java.util.Locale
import scala.collection.JavaConverters.{asJavaIterableConverter, mapAsScalaMapConverter}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConversions.mapAsJavaMap



class CustomDataSourceAnalyzer(session: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  session.listenerManager.register(new CatalogQueryExecutionListener)


  def getFileFormat(formatName: String): FileFormat = {
    formatName.toLowerCase match {
      case "csv" => new CSVFileFormat
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "orc" => new OrcFileFormat
      case "avro" => new AvroFileFormat
      case "json" => new JsonFileFormat
      case "arrow" => new ArrowFileFormat
      case _ => new CSVFileFormat
    }
  }

  def getExternalCatalogOverrideSchema(options: Map[String,String], schema:StructType):StructType = {
    options.get("dt.override") match {
      case None => schema
      case Some(value) =>
        val colNames = value.split(",").map(x => x.toLowerCase.trim).toSeq
        val newFields = schema.fields.map(f => {
          if(colNames.contains(f.name)){
            f.copy(dataType = StringType)
          }else{
            f
          }
        }).toSeq
        StructType.apply(newFields)
    }
  }

  private def isHiveCreatedView(metadata: CatalogTable): Boolean = {
    // For views created by hive without explicit column names, there will be auto-generated
    // column names like "_c0", "_c1", "_c2"...
    metadata.viewQueryColumnNames.isEmpty &&
      metadata.schema.fieldNames.exists(_.matches("_c[0-9]+"))
  }




  private def getViewColumns(metadata: CatalogTable): Seq[NamedExpression] = {
    val projectList = if (!isHiveCreatedView(metadata)) {
      //      val viewColumnNames = if (metadata.viewQueryColumnNames.isEmpty) {
      //        // For view created before Spark 2.2.0, the view text is already fully qualified, the plan
      //        // output is the same with the view output.
      //        metadata.schema.fieldNames.toSeq
      //      } else {
      //        assert(metadata.viewQueryColumnNames.length == metadata.schema.length)
      //        metadata.viewQueryColumnNames
      //      }
      val viewColumnNames =  metadata.schema.fieldNames.toSeq

      // For view queries like `SELECT * FROM t`, the schema of the referenced table/view may
      // change after the view has been created. We need to add an extra SELECT to pick the columns
      // according to the recorded column names (to get the correct view column ordering and omit
      // the extra columns that we don't require), with UpCast (to make sure the type change is
      // safe) and Alias (to respect user-specified view column names) according to the view schema
      // in the catalog.
      // Note that, the column names may have duplication, e.g. `CREATE VIEW v(x, y) AS
      // SELECT 1 col, 2 col`. We need to make sure that the matching attributes have the same
      // number of duplications, and pick the corresponding attribute by ordinal.
      val viewConf = View.effectiveSQLConf(metadata.viewSQLConfigs, false)
      val normalizeColName: String => String = if (viewConf.caseSensitiveAnalysis) {
        identity
      } else {
        _.toLowerCase(Locale.ROOT)
      }
      val nameToCounts = viewColumnNames.groupBy(normalizeColName).mapValues(_.length)
      val nameToCurrentOrdinal = scala.collection.mutable.HashMap.empty[String, Int]
      val viewDDL = buildViewDDL(metadata, false)

      viewColumnNames.zip(metadata.schema).map { case (name, field) =>
        val normalizedName = normalizeColName(name)
        val count = nameToCounts(normalizedName)
        val ordinal = nameToCurrentOrdinal.getOrElse(normalizedName, 0)
        nameToCurrentOrdinal(normalizedName) = ordinal + 1
        val col = GetViewColumnByNameAndOrdinal(
          metadata.identifier.toString, name, ordinal, count, viewDDL)
        Alias(UpCast(col, field.dataType), field.name)(explicitMetadata = Some(field.metadata))
      }
    } else {
      // For view created by hive, the parsed view plan may have different output columns with
      // the schema stored in metadata. For example: `CREATE VIEW v AS SELECT 1 FROM t`
      // the schema in metadata will be `_c0` while the parsed view plan has column named `1`
      metadata.schema.zipWithIndex.map { case (field, index) =>
        val col = GetColumnByOrdinal(index, field.dataType)
        Alias(UpCast(col, field.dataType), field.name)(explicitMetadata = Some(field.metadata))
      }
    }
    //    projectList.map(at => if(at.isInstanceOf[Alias]){
    //      at
    //    }else{
    //      at
    //    })
    projectList
  }


  private def buildViewDDL(metadata: CatalogTable, isTempView: Boolean): Option[String] = {
    if (isTempView) {
      None
    } else {
      val viewName = metadata.identifier.unquotedString
      val viewText = metadata.viewText.get
      val userSpecifiedColumns =
        if (metadata.schema.fieldNames.toSeq == metadata.viewQueryColumnNames) {
          " "
        } else {
          s" (${metadata.schema.fieldNames.mkString(", ")}) "
        }
      Some(s"CREATE OR REPLACE VIEW $viewName${userSpecifiedColumns}AS $viewText")
    }
  }

  def getViewPlan(table: V2Table, relation: Option[DataSourceV2Relation] = None): LogicalPlan = {
    val viewText = table.v1Table.viewText.getOrElse {
      throw new IllegalStateException("Invalid view without text.")
    }
    val viewConfigs = table.v1Table.viewSQLConfigs
    val origin = Origin(
      objectType = Some("VIEW"),
      objectName = Some(table.v1Table.qualifiedName)
    )

    val parsedPlan = SQLConf.withExistingConf(View.effectiveSQLConf(viewConfigs, false)) {
      try {
        CurrentOrigin.withOrigin(origin) {
          (new CustomSparkSQLParser()).parseQuery(viewText)
        }
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewText(viewText, table.v1Table.qualifiedName)
      }
    }
    val projectList = getViewColumns(table.v1Table)
    //val secureProjection = getSecureProjectList(projectList, table.v1Table)
    // val resolvedPlan = apply(Project(projectList, parsedPlan))
    val parsedPlanWithoutSecureAttribute = CLSUtils.removeSecureProjection(parsedPlan)
    val child = Project(projectList, parsedPlanWithoutSecureAttribute)

//    val details = CLSUtils.getCatalogTableDetails(table)
//    val secureTable = CLSUtils.getSecureTableFrom(details._1,details._2,details._3)
//    val secureViewPlan  = CLSUtils.getSecureLeafPlan(secureTable, leafPlan = child)

    CLSUtils.tagViewPlan(plan = child)
    val newPlan = (new CLSSecRule(session)).apply(child)
   // CLSUtils.tagViewPlan(plan = child)
    if (!isHiveCreatedView(table.v1Table))
      newPlan.setTagValue(TreeNodeTag[String]("custom-view-projection"), "true")

    CLSUtils.tagViewPlan(plan = newPlan)
    val newChild = session.sessionState.analyzer.executeAndCheck(newPlan, new QueryPlanningTracker())
    val secureViewPlan = CLSUtils.getSecureViewPlan(View(desc = table.v1Table, isTempView = false, child = newChild))
    CLSUtils.tagViewPlan(plan = secureViewPlan)
    println("Returning View")
    CustomView(desc = table.v1Table,secureViewPlan )
    //val resolvedPlan = session.sharedState.sparkContext.



    //secureViewPlan
  }



  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {

    //    case c:CustomInsertIntoHadoopFsRelationCommand =>
    //      c.setAnalyzed()
    //      c




    case u@UnresolvedRelation(multipartIdentifier: Seq[String], _, _) =>
      println("Inside Unresolved " + u.toString())
      val catalogName = session.sessionState.catalogManager.currentCatalog.name()

      val res = if (multipartIdentifier.size == 3) {
        (multipartIdentifier(0), multipartIdentifier(1), multipartIdentifier(2))
      } else if (multipartIdentifier.size == 2) {
        (catalogName, multipartIdentifier(0), multipartIdentifier(1))
      } else {
        (catalogName, "default", multipartIdentifier(0))
      }
      val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(res._1).asTableCatalog
      val plugin = session.sessionState.catalogManager.catalog(res._1)
      if (res._1.equalsIgnoreCase("spark_catalog")) {
        if (SparkSession.active.sessionState.catalog.tableExists(TableIdentifier(res._3, Some(res._2)))) {
          val ct = SparkSession.active.sessionState.catalog.getTableMetadata(TableIdentifier(res._3, Some(res._2)))
          val v2Table = V2Table(ct)
          DataSourceV2Relation.create(v2Table, Some(sessionCatalog), Some(Identifier.of(Seq(res._2).toArray, res._3)))
        } else if (SparkSession.active.sessionState.catalog.getTempView(res._3).isDefined) {
          println("Inside temp view block")
          SparkSession.active.sessionState.catalog.getTempView(res._3).get
        } else if (SparkSession.active.sessionState.catalog.getGlobalTempView(res._3).isDefined) {
          println("Inside global temp view block")
          SparkSession.active.sessionState.catalog.getGlobalTempView(res._3).get
        } else {
          u
        }
      } else {
        // if(u.options.containsKey(""))
        val tc = sessionCatalog.loadTable(Identifier.of(Seq(res._2).toArray, res._3))
        val viewc = sessionCatalog.loadTable(Identifier.of(Seq(res._2).toArray, res._3), null)
        if (tc == null && viewc != null) {
          getViewPlan(viewc.asInstanceOf[V2Table])
        } else {
          tc match {
            case v2Table: V2Table =>
              val provider = v2Table.v1Table.provider.getOrElse("custom")
              // val table = tc.asInstanceOf[V2Table]

              if (provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("csv")
                || provider.equalsIgnoreCase("parquet")
                || provider.equalsIgnoreCase("orc")
                || provider.equalsIgnoreCase("avro")
                || provider.equalsIgnoreCase("arrow")) {
                val ds = DataSourceV2Relation.create(table = v2Table.getV2CustomTable, catalog = Some(plugin), identifier = Some(Identifier.of(Seq(v2Table.v1Table.identifier.database.getOrElse("default")).toArray, v2Table.v1Table.identifier.table)), options = v2Table.getTableCaseInsensitiveStringMap)
               // ListenerUtil.copyPlanTagsIfExists(dd, ds)
                if(!CLSUtils.isViewsPlan(u)) {
                  println("secure child should apply")
                  CLSUtils.getSecureDataSource(ds)
                }else{
                  println("secure child should not apply")
                  CLSUtils.tagViewPlan(ds)
                  CLSUtils.tagViewPlan(ds)
                    ds
                }
              } else {
                val dataSource = DataSource(
                  session,
                  // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
                  // inferred at runtime. We should still support it.
                  userSpecifiedSchema = if (v2Table.schema.isEmpty) None else Some(v2Table.schema),
                  partitionColumns = v2Table.v1Table.partitionColumnNames,
                  bucketSpec = v2Table.v1Table.bucketSpec,
                  className = v2Table.v1Table.provider.get,
                  options = v2Table.v1Table.storage.properties++getReadOptionsForExternalSource,
                  catalogTable = Some(v2Table.v1Table))
                LogicalRelation(dataSource.resolveRelation(false), v2Table.v1Table)
              }

            case deltaTableV2: DeltaTableV2 => val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(res._1).asTableCatalog
              //          import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
              //          val (partitionColumns, maybeBucketSpec) = deltaTableV2.partitioning().toSeq.convertTransforms
              //          val dataSource = DataSource(
              //            session,
              //            paths = Seq(deltaTableV2.catalogTable.get.location.toString),
              //            // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
              //            // inferred at runtime. We should still support it.
              //            userSpecifiedSchema = if (deltaTableV2.schema.isEmpty) None else Some(deltaTableV2.schema),
              //            partitionColumns = partitionColumns,
              //            bucketSpec = maybeBucketSpec,
              //            className = deltaTableV2.catalogTable.get.provider.getOrElse("delta"),
              //            options = Map.empty,
              //            catalogTable = Some(deltaTableV2.catalogTable.get))
              //          LogicalRelation(dataSource.resolveRelation(false), deltaTableV2.catalogTable.get)

              val ds = DataSourceV2Relation.create(deltaTableV2, Some(sessionCatalog), Some(Identifier.of(Seq(res._2).toArray, res._3)), options = u.options)
              if (!CLSUtils.isViewsPlan(u) ) {
                if(CDCReader.isCDCRead(u.options)){
                  ds
                }else {
                  println("secure child should apply")
                  CLSUtils.getSecureDataSource(ds)
                }
              } else {
                println("secure child should not apply")
                CLSUtils.tagViewPlan(ds)

                val lr = DeltaRelation.fromV2Relation(deltaTableV2,ds,ds.options)
                CLSUtils.tagSingleViewPlan(lr)
                lr
              }

            case sparkTable: SparkTable =>
              val ds = DataSourceV2Relation.create(sparkTable, Some(sessionCatalog), Some(Identifier.of(Seq(res._2).toArray, res._3)))
              if (!CLSUtils.isViewsPlan(u)) {
                println("secure child should apply")
                CLSUtils.getSecureDataSource(ds)
              } else {
                println("secure child should not apply")
                CLSUtils.tagViewPlan(ds)
                ds
              }

            case _ => u

          }
        }
      }


    case dd@DataSourceV2Relation(table: V2Table, output: Seq[AttributeReference], _, _, options: CaseInsensitiveStringMap) =>

      println("Inside DataSourceV2Relation ")
      if (table.v1Table.tableType == CatalogTableType.VIEW) {
        return getViewPlan(table)
      }
      val provider = table.v1Table.provider.getOrElse("custom")
      var dataSource = DataSource(
        session,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.v1Table.partitionColumnNames,
        bucketSpec = table.v1Table.bucketSpec,
        className = table.v1Table.provider.get,
        options = table.v1Table.storage.properties ++ options.asScala.toMap ++ getReadOptionsForExternalSource,
        catalogTable = None)

      val catalogName = table.getCatalogName
      val plugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)

      if (provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("csv")
        || provider.equalsIgnoreCase("parquet")
        || provider.equalsIgnoreCase("orc")
        || provider.equalsIgnoreCase("avro")
        || provider.equalsIgnoreCase("arrow")
        || provider.equalsIgnoreCase("textfile")) {

//        val schemaColName = table.v1Table.dataSchema.map(f => f.name)
//        val partSchemaColNames = table.v1Table.partitionSchema.map(f => f.name)
//        val defaultTableSize = SparkSession.active.sessionState.conf.defaultSizeInBytes
//        val fileCatalog = new CustomCatalogFileIndex(
//          SparkSession.active,
//          table.v1Table,
//          table.v1Table.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))

        //       val tablePath  = new Path(table.v1Table.location.getPath)
        //        val fileCatalog = new MetadataLogFileIndex(SparkSession.active, tablePath,
        //          Map.empty[String, String], Some(table.v1Table.schema))

        //val source = DataSource.lookupDataSource("hive", SparkSession.active.sessionState.conf)
        //val fileFormat = source.getConstructor().newInstance().asInstanceOf[FileFormat]
//
//        val relation = LogicalRelation(relation = HadoopFsRelation(
//          location = fileCatalog,
//          partitionSchema = table.v1Table.partitionSchema,
//          dataSchema = table.v1Table.dataSchema,
//          fileFormat = ff,
//          options = mapHiveCSVPropertiesToSparkOption(table.v1Table, ff),
//          bucketSpec = None
//        )(SparkSession.active), isStreaming = false)
//
//        val resolvedLeafPlan = relation.copy(output = output)
//        resolvedLeafPlan
        val ds = DataSourceV2Relation.create(table = table.getV2CustomTable, catalog = Some(plugin), identifier = Some(Identifier.of(Seq(table.v1Table.identifier.database.getOrElse("default")).toArray, table.v1Table.identifier.table)), options = table.getTableCaseInsensitiveStringMap)
        ListenerUtil.copyPlanTagsIfExists(dd, ds)
        //ds.copy(output = dd.output)
        CLSUtils.getSecureDataSource(ds.copy(output = dd.output))

      } else {
        val leafPlan = if (provider.equalsIgnoreCase("custom")) {
          if(options.asScala.contains("dt.override") && !options.asScala.contains("override.complete")){
            val overridenSchema = getExternalCatalogOverrideSchema(options.asScala.toMap, table.v1Table.schema)
            dataSource = dataSource.copy(userSpecifiedSchema = Some(overridenSchema))
            val newCt = table.v1Table.copy(schema = overridenSchema)
            val table1 = V2Table(newCt)
            val newOptions: Map[String, String] = options.asScala.toMap ++ Map("override.complete" -> "true")
            val newCaseOptions = new CaseInsensitiveStringMap(mapAsJavaMap(newOptions))
            SparkSession.active.sessionState.analyzer.execute(dd.copy(table = table1, options = newCaseOptions))
          }
          options.asScala.get("source.pushdown.enabled") match {
            case Some("true") =>
              val optionsMap = dataSource.options
              val ds = DataSourceV2Relation.create(table = table.getV2CustomTable, catalog = Some(plugin), identifier = Some(Identifier.of(Seq(table.v1Table.identifier.database.getOrElse("default")).toArray, table.v1Table.identifier.table)), options = new CaseInsensitiveStringMap(optionsMap))
              ds.copy(output = dd.output)
            case Some("false") =>
              LogicalRelation(dataSource.resolveRelation(false),table.v1Table)
            case None =>
              LogicalRelation(dataSource.resolveRelation(false),table.v1Table)

          }
        } else {
          LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
        }
        leafPlan match {
          case d: DataSourceV2Relation => d
          case l: LogicalRelation =>
            val resolvedLeafPlan = l.copy(output = output)
            resolvedLeafPlan.resolved
            resolvedLeafPlan
        }

      }

    //code looks like it will throw error, if its nt V2Table but just above we solved it for V2Table only
    case x@Project(p, child@SubqueryAlias(identifier, child1: DataSourceV2Relation))
      if child1.catalog.isDefined =>

      println("Inside Project over DataSourceV2Relation")
      //  x.setAnalyzed()

      child1.table match {
        case sparkTable: SparkTable => x
        case v2Table: V2Table =>
          val table = child1.table.asInstanceOf[V2Table]
          if (table.v1Table.tableType == CatalogTableType.VIEW) {
            return getViewPlan(table)
          }
          val provider = child1.table.asInstanceOf[V2Table].v1Table.provider.getOrElse("custom")
          val dataSource = DataSource(
            session,
            // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
            // inferred at runtime. We should still support it.
            userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
            partitionColumns = table.v1Table.partitionColumnNames,
            bucketSpec = table.v1Table.bucketSpec,
            className = table.v1Table.provider.get,
            options = table.v1Table.storage.properties ++ child1.options.asScala.toMap ++ getReadOptionsForExternalSource,
            catalogTable = Some(table.v1Table))

          if (provider.equalsIgnoreCase("hive") || provider.equalsIgnoreCase("csv")
            || provider.equalsIgnoreCase("parquet")
            || provider.equalsIgnoreCase("orc")
            || provider.equalsIgnoreCase("avro")
            || provider.equalsIgnoreCase("arrow")
            || provider.equalsIgnoreCase("textfile")) {
            val schemaColName = table.v1Table.dataSchema.map(f => f.name)
            val partSchemaColNames = table.v1Table.partitionSchema.map(f => f.name)
            val dataCols = child1.output.filter(p => schemaColName.contains(p.name))
            val partCols = child1.output.filter(p => partSchemaColNames.contains(p.name))
            val defaultTableSize = SparkSession.active.sessionState.conf.defaultSizeInBytes
            val fileCatalog = new CustomCatalogFileIndex(
              SparkSession.active,
              table.v1Table,
              table.v1Table.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))

            //val source = DataSource.lookupDataSource("hive", SparkSession.active.sessionState.conf)
            //val fileFormat = source.getConstructor().newInstance().asInstanceOf[FileFormat]
            val ff = if (provider.equalsIgnoreCase("hive")) {
              getHiveTableFileFormat(table.v1Table)
            } else {
              getFileFormat(provider)
            }

//            val relation = LogicalRelation(relation = HadoopFsRelation(
//              location = fileCatalog,
//              partitionSchema = table.v1Table.partitionSchema,
//              dataSchema = table.v1Table.dataSchema,
//              fileFormat = ff,
//              options = mapHiveCSVPropertiesToSparkOption(table.v1Table, ff),
//              bucketSpec = None
//            )(SparkSession.active))
            val relation = apply(child1)
          //  val newRelation = relation.copy(output = child1.output)
          //  val newChild = child.copy(identifier = identifier, child = newRelation)
            val op = x.copy(projectList = p, child = relation)
            op.resolved
            //   op.setAnalyzed()
            op
          } else {
            val relation = if (provider.equalsIgnoreCase("custom")) {
              LogicalRelation(dataSource.resolveRelation(false), table.v1Table)
            } else {
              LogicalRelation(dataSource.resolveRelation(true), table.v1Table)
            }
            val newRelation = relation.copy(output = child1.output, catalogTable = Some(table.v1Table), relation = relation.relation, isStreaming = false)
            val newChild = child.copy(identifier = identifier, child = newRelation)
            val op = x.copy(child = newChild)
            op.resolved
            // op.setAnalyzed()
            op
          }
        case _ => x
      }

    case u: UnresolvedTable =>
      println("Inside UnresolvedTable")
      if (u.multipartIdentifier.size == 3) {
        val catName = u.multipartIdentifier(0)
        val dbName = u.multipartIdentifier(1)
        val tableName = u.multipartIdentifier(2)
        val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
        val tid = Identifier.of(Seq(dbName).toArray, tableName)
        val tc = sessionCatalog.loadTable(tid)
        if (tc == null) {
          val viewCt = sessionCatalog.loadTable(tid, null)
          if (viewCt != null) {
            ResolvedTable.create(sessionCatalog, tid, viewCt)
          } else {
            u
          }

        } else {
          tc match {
            case d: DeltaTableV2 => (ResolvedTable.create(sessionCatalog, u.multipartIdentifier.asIdentifier, d))
            case _ => u
          }
        }
      } else {
        u
      }

    // child.setAnalyzed()
    //  child
    case InsertIntoStatement(u: UnresolvedRelation, m: Map[String, Option[String]], a: Seq[String], q: LogicalPlan, f: Boolean, ip: Boolean, c: Boolean) =>
      println("InsertIntoStatement with UnresolvedRelation")
      val (catalogName, dbName, tableName) = if (u.multipartIdentifier.size == 2) {
        //extract catalog name from conf
        if (SparkSession.active.conf.contains("spark.insert.catalog")) {
          (SparkSession.active.conf.get("spark.insert.catalog"), u.multipartIdentifier.head, u.multipartIdentifier.last)
        } else {
          ("spark_catalog", u.multipartIdentifier.head, u.multipartIdentifier.last)
        }
      } else if (u.multipartIdentifier.size == 3) {
        (u.multipartIdentifier.head, u.multipartIdentifier(1), u.multipartIdentifier.last)
      } else {
        ("spark_catalog", u.multipartIdentifier.head, u.multipartIdentifier.last)
      }
      val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catalogName).asTableCatalog
      val catalogTable = sessionCatalog.loadTable(Identifier.of(Seq(dbName).toArray, tableName))
      val ct = catalogTable.asInstanceOf[V2Table].v1Table
     // q.setAnalyzed()

      if (ct.provider.getOrElse("custom").equalsIgnoreCase("custom")) {
        val table = ct
        val dataSource = DataSource(
          session,
          // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
          // inferred at runtime. We should still support it.
          userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
          partitionColumns = table.partitionColumnNames,
          bucketSpec = table.bucketSpec,
          className = table.provider.get,
          options = table.storage.properties ++ u.options.asScala.toMap ++ getReadOptionsForExternalSource,
          catalogTable = Some(table)
        )

        val relation = LogicalRelation(dataSource.resolveRelation(false), table)
        InsertIntoStatement(relation, m, a, q, f, ip, c)

      } else {
        val in = InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(ct.storage.locationUri.get.toString),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = ct.partitionColumnNames.map(UnresolvedAttribute.quoted),
          bucketSpec = None,
          fileFormat = getFileFormat(ct.provider.getOrElse("csv")),
          options = Map.empty,
          query = q,
          mode = SaveMode.Append,
          catalogTable = Some(ct),
          fileIndex = None,
          outputColumnNames = ct.schema.map(f => f.name)
        )
        tagInsetIntoHadoopFsWithCatalogDetails(in, ct)
        in
      }

    case i@InsertIntoStatement(d: DataSourceV2Relation, m: Map[String, Option[String]], a: Seq[String], q: LogicalPlan, f: Boolean, ip: Boolean, c: Boolean) => {
      val retPlan = new DeltaAnalysis(SparkSession.active).apply(CustomResolveInsertInto(i))
      println("Insert into plan output for listener "+ ListenerUtil.getSQLTextIfExists(i))
      ListenerUtil.copyPlanTagsIfExists(i, retPlan)
      retPlan
    }


    case ab@AppendData(table@DataSourceV2Relation(v: DeltaTableV2, _, _, _, _), p: Project, writeOptions, isByName, write, analyzedQuery) =>
      print("Insie append data")
      if (v.v1Table.provider.isDefined && v.v1Table.provider.get.equalsIgnoreCase("delta"))
        ab.copy(analyzedQuery = Some(p))
      else
        ab


    case p: LogicalPlan => p resolveOperatorsUp {


      case vu:ViewUnresolvedRelation =>
        println("For ViewUnresolvedRelation")
        apply(vu.u)

      //      case prj@Project(projectList, s@SubqueryAlias(_, view: View)) =>
      //      //  prj.copy(view.output)
      //        prj.setAnalyzed()
      //        val ats = getResolvedProjectAttributes(prj, view)
      //        prj.copy(ats)


      case pl:LogicalPlan if CLSUtils.isViewsPlan(pl) =>
        println("For View Plan came inside secure Projection")
        new CLSSecRule(session).apply(pl)

      case ds@DataSourceV2ScanRelation(relation: DataSourceV2Relation, scan, output, keyGroupedPartitioning, ordering) =>
        println("this is DataSourceV2Scan")
        println(s"${ds.toString()}")
        ds

      case d: DataSourceV2Relation =>
        println("this is DataSourceV2 " + d.toString())
        if (d.table.isInstanceOf[DeltaTableV2] && d.getTagValue(DeltaRelation.KEEP_AS_V2_RELATION_TAG).isEmpty ) {
          println("Inside DataSourceV2 for DeltaTable")
          println("Making map "+ d.options.asScala.mkString(","))
          DeltaRelation.fromV2Relation(d.table.asInstanceOf[DeltaTableV2], d, d.options)

        } else {
          if (d.getTagValue(TreeNodeTag[String]("centrify-resolver")).isEmpty) {
            d.setTagValue(TreeNodeTag[String]("centrify-resolver"), "resolved")
            if (!d.table.isInstanceOf[V2CustomTable]) {
              apply(d)
            } else {
              d
            }
          } else {
            if(CDCReader.isCDCRead(d.options)){
              d
            }else {
              CLSUtils.getSecureRelation(d)
            }
          }
        }

      case u: UnresolvedRelation =>
        println("this is for view " + u.toString())
        if (u.getTagValue(TreeNodeTag[String]("centrify-resolver")).isEmpty) {
          u.setTagValue(TreeNodeTag[String]("centrify-resolver"), "resolved")
          apply(u)
        }else{
          u
        }



      case unresolvedInlineTable: UnresolvedInlineTable =>
        ResolveInlineTables(unresolvedInlineTable)

      //      case relationTimeTravel@RelationTimeTravel(u:UnresolvedRelation, _, _) =>
      //        val resolvedPlan = resolveTimeTravelTable(SparkSession.active,u,"RESTORE")
      //        relationTimeTravel.copy(relation = resolvedPlan)

      case u: UnresolvedLeafNode =>
        print(u.toString())
        if (u.getTagValue(TreeNodeTag[String]("centrify-resolver")).isEmpty) {
          u.setTagValue(TreeNodeTag[String]("centrify-resolver"), "resolved")
          apply(u)
        }else{
          u
        }

      //      case pr@Project(plist, p@Project(projectList, child)) =>
      //
      //        val res =  pr.copy(projectList, p)
      //        res.resolved
      //        res.setAnalyzed()
      //        res
      case p: LogicalPlan =>
        println("Default plan is " + p.toString())

        p match {

          case c@CreateTable(tabledesc, SaveMode.Append, Some(query)) =>
            CustomCreateDataSourceTableAsSelectCommand(
              tabledesc.identifier.catalog.getOrElse(session.sessionState.catalogManager.currentCatalog.name()),
              tabledesc,
              c.mode,
              query = query,
              query.output.map(a => a.name)
            )

          case tc@TableChanges(child, fnName, cdcAttr) if tc.child.resolved =>
            println("tc child plan is "+child.toString())
            tc.toReadQuery
           // getDeltaTableV2RelationFrom(child)


          case lr@LogicalRelation(relation, output, catalogTable, isStreaming) =>
            println("Inside Logical Relation " + relation.toString)
            if(lr.relation.isInstanceOf[DeltaCDFRelation]) {
              lr
            } else {
              lr
              //println("")
             CLSUtils.getSecureRelation(lr)
            }



          case d: DeserializeToObject =>
            if (!d.resolved) {
              val deserExp = ResolveReferences.resolveExpressionByPlanChildren(d.deserializer, d)
              val resolveAttr = ResolveReferences.resolveExpressionByPlanChildren(d.outputObjAttr, d).asInstanceOf[Attribute]
              d.copy(deserializer = deserExp, outputObjAttr = resolveAttr)
            } else {
              d
            }

          case m: DeltaMergeInto =>
            PreprocessTableMerge(session.sqlContext.conf).apply(m)

          case u: DeltaUpdateTable =>
            PreprocessTableUpdate.apply(session.sqlContext.conf).toCommand(u)

          case d: DeltaDelete =>
            d.condition.foreach { cond =>
              if (SubqueryExpression.hasSubquery(cond)) {
                throw DeltaErrors.subqueryNotSupportedException("DELETE", cond)
              }
            }
          //  d.setAnalyzed()
            println("value of resolved is " + d.resolved.toString)
            // val pl = ResolveReferences(d)
            DeleteCommand(d)


          case rtas@ReplaceTableAsSelect(ResolvedIdentifier(catalog, ident), partitioning, query, tableSpec, writeOptions, orCreate, isAnalyzed) => {
            val optionString = writeOptions.map(t => t._1 + "::" + t._2).mkString("||")
            println("RTAS Option String: " + optionString)
//            if (tableSpec.provider.isDefined && tableSpec.provider.get.equalsIgnoreCase("delta")
//              ||
            if ( catalog.name().equalsIgnoreCase("ecat")) {
              CreateTableAsSelect(ResolvedIdentifier(catalog, ident), partitioning = partitioning, query = query, tableSpec = tableSpec, writeOptions = writeOptions, ignoreIfExists = false, isAnalyzed = isAnalyzed)
            } else {
              rtas
            }
          }


          case ctas@CreateTableAsSelect(ResolvedIdentifier(catalog, ident), partitioning, query, tableSpec,writeOptions, ignoreIfExists, isAnalyzed) =>
            val optionString = ctas.writeOptions.map(t => t._1 + "::" + t._2).mkString("||")
            println("CTAS Option String: " + optionString)
            println("Analyzer check for listener ctas"+ ListenerUtil.getSQLTextIfExists(ctas))
            println("Analyzer check for listener ctas query"+ ListenerUtil.getSQLTextIfExists(query))

            val providerValue = getActualProvider(catalog,ident,tableSpec)
            if(catalog.name().equalsIgnoreCase("ecat")){
              val properties = getOldTableProps(catalog, ident, tableSpec)
              OverWriteToExternalSource.createAndOverWritePlan(query, catalog, ident, properties, "custom", ctas.writeOptions, partitioning)
            }else {
              ctas
            }

          case in: InsertIntoStatement =>
            in //return as it is

          case ab@AppendData(table@DataSourceV2Relation(v: DeltaTableV2, _, _, _, _), _, _, _, _, _) =>
            println("Inside append data")
            if (v.v1Table.provider.isDefined && v.v1Table.provider.get.equalsIgnoreCase("delta")) {
              println("Inside delta analyzer block")
              ab.copy(analyzedQuery = Some(ab.query))
            } else {
              ab
            }

          case abd@AppendData(table: DataSourceV2Relation, _, _, _, _, _) =>
            println("Inside append data")
            val abpl = abd.copy(analyzedQuery = Some(abd.query))
            appendIntoV2Table(abpl, table)

          case overwriteByExpression@OverwriteByExpression(table: DataSourceV2Relation, deleteExpr, query, writeOptions, isByName, _, analyzedQuery) =>
            //  val overwriteByExpressionPl = overwriteByExpression.copy(analyzedQuery = Some(query))
            println("Inside Overwrite by Expression")
            table.table match {
              case v2Table: V2Table =>
                val catalogName = v2Table.v1Table.identifier.catalog.getOrElse("hive")
                val catalogPlugin = SparkSession.active.sessionState.catalogManager.catalog(catalogName)
                val resId = ResolvedIdentifier(
                  catalogPlugin,
                  Identifier.of(Array(v2Table.v1Table.identifier.database.getOrElse("default")).toArray, v2Table.v1Table.identifier.table)
                )
                ReplaceTableAsSelect(resId, Seq.empty[Transform], query, TableSpec(
                  provider = v2Table.v1Table.provider,
                  properties = (Map("provider" -> v2Table.v1Table.provider.getOrElse("hive"))),
                  location = Some(v2Table.v1Table.location.toString),
                  options = writeOptions,
                  comment = None, serde = None, external = v2Table.v1Table.tableType == CatalogTableType.EXTERNAL
                ), writeOptions, false)

              case _ => overwriteByExpression
            }
          case view: View =>
            println("view found")
            view

          case prj@Project(projectList, s@SubqueryAlias(id: Identifier, view: View)) =>
            println("project over view found")
            prj.copy(view.output)


          case _ =>
          //  p
            val pl = ResolveReferences(p)
            pl.resolved
            //  pl.setAnalyzed()
            pl
        }
    }
  }
  private def getTextFileFormat(table:CatalogTable):FileFormat ={
    val tblprops = table.properties
    val storageProps = table.storage.properties
    if(tblprops.contains("option.delimiter") || tblprops.contains("field.delim") || tblprops.contains("delimiter")
    || storageProps.contains("option.delimiter") || storageProps.contains("field.delim") || storageProps.contains("delimiter")){
      new CSVFileFormat
    }else{
      new TextFileFormat
    }
  }

  def getHiveTableFileFormat(table: CatalogTable): FileFormat = {
    table.storage.properties("fileformat").toLowerCase match {
      case "orc" => new OrcFileFormat
      case "parquet" => new ParquetFileFormat
      case "csv" => new CSVFileFormat
      case "avro" => new AvroFileFormat
      case "json" => new JsonFileFormat
      case "text" => new CSVFileFormat
      case "textfile" => getTextFileFormat(table)
      case "arrow" => new ArrowFileFormat
      case "_" => throw new IllegalAccessException("invalid format")
    }
  }

  def appendIntoV2Table(ab: AppendData, ds: DataSourceV2Relation): LogicalPlan = {
    ds.table match {
      case v2: V2Table =>
        val provider = v2.v1Table.provider.getOrElse("hive")
        val table = v2.v1Table
        val ct = v2.v1Table
        if (provider.equalsIgnoreCase("custom")) {

          val dataSource = DataSource(
            session,
            // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
            // inferred at runtime. We should still support it.
            userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
            partitionColumns = table.partitionColumnNames,
            bucketSpec = table.bucketSpec,
            className = table.provider.get,
            options = table.properties ++ ab.writeOptions.toMap ++ getReadOptionsForExternalSource,
            catalogTable = Some(table)
          )
          val columnNames = v2.v1Table.schema.fieldNames
          val relation = LogicalRelation(dataSource.resolveRelation(false), table)
          val ins = InsertIntoStatement(relation, Map.empty[String, Option[String]], columnNames, ab.query, false, false)
          ListenerUtil.copyPlanTagsIfExists(ab, ins)
          ins

        } else {
          val ff = if (provider.equalsIgnoreCase("hive")) {
            getHiveTableFileFormat(ct)
          } else {
            getFileFormat(provider)
          }
          val in = InsertIntoHadoopFsRelationCommand(
            outputPath = new Path(ct.storage.locationUri.get.toString),
            staticPartitions = Map.empty,
            ifPartitionNotExists = false,
            partitionColumns = ct.partitionColumnNames.map(UnresolvedAttribute.quoted),
            bucketSpec = None,
            fileFormat = ff,
            options = mapHiveCSVPropertiesToSparkOption(ct, ff),
            query = ab.analyzedQuery.get,
            mode = SaveMode.Append,
            catalogTable = None,
            fileIndex = None,
            outputColumnNames = ct.schema.map(f => f.name)
          )
          tagInsetIntoHadoopFsWithCatalogDetails(in,ct)
          ListenerUtil.copyPlanTagsIfExists(ab, in)
          ListenerUtil.setTableNameInPlan(in, ct.qualifiedName)
          in
        }
      case st: BestEffortStagedTable =>
        if (st.table.isInstanceOf[V2Table]) {
          val catalogTable = st.table.asInstanceOf[V2Table].v1Table
          if (catalogTable.provider.isDefined && catalogTable.provider.get.equalsIgnoreCase("custom")) {
            val dataSource = DataSource(
              session,
              // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
              // inferred at runtime. We should still support it.
              userSpecifiedSchema = if (catalogTable.schema.isEmpty) None else Some(catalogTable.schema),
              partitionColumns = catalogTable.partitionColumnNames,
              bucketSpec = catalogTable.bucketSpec,
              className = catalogTable.provider.get,
              options = catalogTable.storage.properties ++ ab.writeOptions ++ getOverWriteOptionsForExternalSource,
              catalogTable = Some(catalogTable)
            )
            val columnNames = catalogTable.schema.fieldNames
            val relation = LogicalRelation(dataSource.resolveRelation(false), catalogTable)
            InsertIntoStatement(relation, Map.empty[String, Option[String]], columnNames, ab.query, true, false)

          } else {
            ab
          }
        } else {
          ab
        }
      case _ => ab
    }
  }

  def getResolvedProjectAttributes(prj: Project, view: View): Seq[Attribute] = {
    view.output.filter(at => prj.projectList.map(pat => pat.name).contains(at.name))
  }

  def mapHiveCSVPropertiesToSparkOption(ct: CatalogTable, fileFormat: FileFormat): Map[String, String] = {
    var tblProps = ct.properties

    //tblProps.
    if (fileFormat.isInstanceOf[CSVFileFormat]) {
      if (!tblProps.contains("option.delimiter")) {
        tblProps = tblProps ++ Map("delimiter" -> tblProps.getOrElse("field.delim", ","))
      }

      if (!tblProps.contains("option.quote")) {
        tblProps = tblProps ++ Map("quote" -> tblProps.getOrElse("quoteChar", '\"'.toString))
      }

      if (!tblProps.contains("option.escape")) {
        tblProps = tblProps ++ Map("escape" -> tblProps.getOrElse("escape.delim", '\\'.toString))
      }

      if (!tblProps.contains("option.header")) {
        //tblProps.getOrElse("skip")
        tblProps = tblProps ++ Map("header" -> tblProps.getOrElse("hasheaders", "false"))
      }

      if (!tblProps.contains("option.lineSep")) {
        //tblProps.getOrElse("skip")
        tblProps = tblProps ++ Map("lineSep" -> tblProps.getOrElse("recorddelimiter", "\n"))
      }

      tblProps
    } else {
      tblProps
    }

  }

  private def resolveTimeTravelTable(
                                      sparkSession: SparkSession,
                                      ur: UnresolvedRelation,
                                      commandName: String): LogicalPlan = {
    // Since TimeTravel is a leaf node, the table relation within TimeTravel won't be resolved
    // automatically by the Apache Spark analyzer rule `ResolveRelations`.
    // Thus, we need to explicitly use the rule `ResolveRelations` to table resolution here.
    EliminateSubqueryAliases(sparkSession.sessionState.analyzer.ResolveRelations(ur)) match {
      case _: View =>
        // If the identifier is a view, throw not supported error
        throw DeltaErrors.notADeltaTableException(commandName)
      case tableRelation if tableRelation.resolved =>
        tableRelation
      case _ =>
        // If the identifier doesn't exist as a table, try resolving it as a path table.
        ResolveDeltaPathTable.resolveAsPathTableRelation(sparkSession, ur).getOrElse {
          ur.tableNotFound(ur.multipartIdentifier)
        }
    }
  }



  def getOverWriteOptionsForExternalSource: Map[String, String] ={
    Map(
      "source.external.catalog" -> "true",
      "write.mode" -> "CREATE"
    )
  }


  def getReadOptionsForExternalSource:Map[String,String] ={
    Map(
      "source.external.catalog" -> "true"
    )
  }

  def getActualProvider(catalog: CatalogPlugin, identifier: Identifier, tableSpec: TableSpecBase): String = {
    if (catalog.asTableCatalog.tableExists(identifier)) {
      // catalog.name()
      catalog.asTableCatalog.loadTable(identifier) match {
        case v2Table: V2Table => v2Table.v1Table.provider.get
        case deltaTableV2: DeltaTableV2 => deltaTableV2.v1Table.provider.get
        case sparkTable: SparkTable => "iceberg"
      }
    } else {
      val properties = convertTableProperties(tableSpec)
      properties.getOrElse("provider", "hive")
    }
  }

  def convertTableProperties(t: TableSpecBase): Map[String, String] = {
    val props = convertTableProperties(
      t.properties, t.serde, t.location, t.comment,
      t.provider, t.external)
    withDefaultOwnership(props)
  }


  private def convertTableProperties(
                                      properties: Map[String, String],
                                      serdeInfo: Option[SerdeInfo],
                                      location: Option[String],
                                      comment: Option[String],
                                      provider: Option[String],
                                      external: Boolean = false): Map[String, String] = {
    properties ++
      convertToProperties(serdeInfo) ++
      (if (external) Some(TableCatalog.PROP_EXTERNAL -> "true") else None) ++
      provider.map(TableCatalog.PROP_PROVIDER -> _) ++
      comment.map(TableCatalog.PROP_COMMENT -> _) ++
      location.map(TableCatalog.PROP_LOCATION -> _)
  }

  /**
   * Converts Hive Serde info to table properties. The mapped property keys are:
   *  - INPUTFORMAT/OUTPUTFORMAT: hive.input/output-format
   *  - STORED AS: hive.stored-as
   *  - ROW FORMAT SERDE: hive.serde
   *  - SERDEPROPERTIES: add "option." prefix
   */
  private def convertToProperties(serdeInfo: Option[SerdeInfo]): Map[String, String] = {
    serdeInfo match {
      case Some(s) =>
        s.formatClasses.map { f =>
          Map("hive.input-format" -> f.input, "hive.output-format" -> f.output)
        }.getOrElse(Map.empty) ++
          s.storedAs.map("hive.stored-as" -> _) ++
          s.serde.map("hive.serde" -> _) ++
          s.serdeProperties.map {
            case (key, value) => TableCatalog.OPTION_PREFIX + key -> value
          }
      case None =>
        Map.empty
    }
  }

  def getOldTableProps(catalog: CatalogPlugin, identifier: Identifier, tableSpec: TableSpecBase): Map[String, String] = {
    if (catalog.asTableCatalog.tableExists(identifier)) {
      // catalog.name()
      catalog.asTableCatalog.loadTable(identifier) match {
        case v2: V2Table => v2.v1Table.properties
        case dt: DeltaTableV2 => dt.v1Table.properties
      }
    } else {
      val properties = convertTableProperties(tableSpec)
      properties
    }
  }

  def tagInsetIntoHadoopFsWithCatalogDetails(plan:InsertIntoHadoopFsRelationCommand, table:CatalogTable): Unit = {
    plan.setTagValue(TreeNodeTag[String]("catalog-details"), s"${table.qualifiedName}")
  }

  def getCatalogDetailsFromInsertIntoHadoopFs(in:InsertIntoHadoopFsRelationCommand):Option[String]={
    in.getTagValue(TreeNodeTag[String]("catalog-details"))
  }



}

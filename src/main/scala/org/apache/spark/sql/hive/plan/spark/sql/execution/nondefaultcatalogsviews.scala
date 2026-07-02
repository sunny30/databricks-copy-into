package org.apache.spark.sql.hive.plan.spark.sql.execution

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, GlobalTempView, LocalTempView, ViewType}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, ExternalCatalog}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisOnlyCommand, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.ViewHelper.generateViewProperties
import org.apache.spark.sql.execution.command.{CreateViewCommand, LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.hive.catalog.{FSMetaStoreCatalog, HMSCatalog}
import org.apache.spark.sql.hive.plan.CLSUtils
import org.apache.spark.sql.types.MetadataBuilder

case class NonDefaultCatalogCreateViewCommand(
                              name: TableIdentifier,
                              userSpecifiedColumns: Seq[(String, Option[String])],
                              comment: Option[String],
                              properties: Map[String, String],
                              originalText: Option[String],
                              plan: LogicalPlan,
                              allowExisting: Boolean,
                              replace: Boolean,
                              viewType: ViewType,
                              isAnalyzed: Boolean = false,
                              referredTempFunctions: Seq[String] = Seq.empty)
  extends RunnableCommand /*with AnalysisOnlyCommand*/ {


  override protected def withNewChildrenInternal(
                                                  newChildren: IndexedSeq[LogicalPlan]): NonDefaultCatalogCreateViewCommand = {
    assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  // `plan` needs to be analyzed, but shouldn't be optimized so that caching works correctly.
  //override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(
      isAnalyzed = true,
      // Collect the referred temporary functions from AnalysisContext
      referredTempFunctions = analysisContext.referredTempFunctionNames.toSeq)
  }

  private def isTemporary = viewType == LocalTempView || viewType == GlobalTempView

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catName = name.catalog.getOrElse("spark_catalog")
    val dbName = name.database.getOrElse("default")
    val tableName = name.table
    val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
    val exists = sessionCatalog.tableExists(Identifier.of(Seq(dbName).toArray, tableName))
    val externalCatalog: ExternalCatalog
    = if (SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")) {
      new FSMetaStoreCatalog(
        catName,
        sparkConf = SparkSession.active.sharedState.conf,
        hadoopConfig = SparkSession.active.sharedState.hadoopConf
      )
    } else {
      new HMSCatalog(
        catName,
        sparkConf = SparkSession.active.sharedState.conf,
        hadoopConfig = SparkSession.active.sharedState.hadoopConf
      )
    }
    val analyzedPlan = SparkSession.active.sessionState.analyzer.execute(plan)
    val  createViewAssertion = CLSUtils.validateCreateViewPlan(analyzedPlan)
    if(createViewAssertion){
      throw new IllegalArgumentException("User need permissions on all columns of all the tables in VIEW SQL TEXT")
    }
    val viewPlan = CLSUtils.removeSecureProjection(analyzedPlan)
    if(!exists){
      externalCatalog.createTable(prepareTable(sparkSession, viewPlan),false)
    }

    if(replace){
      externalCatalog.dropTable(dbName, tableName,true, false)
      externalCatalog.createTable(prepareTable(sparkSession, viewPlan),false)
    }
    Seq.empty[Row]
  }


  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

  /**
   * Returns a [[CatalogTable]] that can be used to save in the catalog. Generate the view-specific
   * properties(e.g. view default database, view query output column names) and store them as
   * properties in the CatalogTable, and also creates the proper schema for the view.
   */
  private def prepareTable(session: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    if (originalText.isEmpty) {
      throw QueryCompilationErrors.createPersistedViewFromDatasetAPINotAllowedError()
    }
    val queryPlan = CLSUtils.removeSecureProjection(analyzedPlan)
    println(queryPlan.toString())
    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, queryPlan).schema, session.sessionState.conf)
    val newProperties = generateViewProperties(
      properties, session, queryPlan, aliasedSchema.fieldNames)

    CatalogTable(

      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasedSchema,
      properties = newProperties,
      viewOriginalText = originalText,
      viewText = originalText,
      comment = comment
    )
  }

}


case class NonDefaultCatalogDropViewCommand(catalogName: String,
                                            dbName: String,
                                            tableName: String,
                                            isExists: Boolean) extends LeafRunnableCommand {


  override def run(sparkSession: SparkSession): Seq[Row] = {

    val sessionExternalCatalog = sparkSession.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    //val tc = sessionCatalog.loadTable(Identifier.of(Seq(dbName).toArray, tableName))
    val tableIdent = Identifier.of(Seq(dbName).toArray, tableName)
    if (sessionExternalCatalog.tableExists(tableIdent)) {
      sessionExternalCatalog.dropTable(tableIdent)
      Seq.empty[Row]
    } else {
      if (isExists) {
        Seq.empty[Row]
      } else {
        throw QueryCompilationErrors.noSuchTableError(
          Seq(catalogName + "." + dbName + "." + tableName))
      }
    }

  }


}



case class NonDefaultCatalogAlterViewQueryCommand(
                                               name: TableIdentifier,
                                               originalText: Option[String],
                                               plan: LogicalPlan,
                                               isAnalyzed: Boolean = false,
                                               referredTempFunctions: Seq[String] = Seq.empty)
  extends RunnableCommand with AnalysisOnlyCommand{


  override protected def withNewChildrenInternal(
                                                  newChildren: IndexedSeq[LogicalPlan]): NonDefaultCatalogAlterViewQueryCommand = {
  //  assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

  def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(
      isAnalyzed = true,
      // Collect the referred temporary functions from AnalysisContext
      referredTempFunctions = analysisContext.referredTempFunctionNames.toSeq)
  }


  override def run(sparkSession: SparkSession): Seq[Row] = {

    val catName = name.catalog.getOrElse("spark_catalog")
    val dbName = name.database.getOrElse("default")
    val tableName = name.table

    val sessionCatalog = SparkSession.active.sessionState.catalogManager.catalog(catName).asTableCatalog
    val exists = sessionCatalog.tableExists(Identifier.of(Seq(dbName).toArray, tableName))
    val externalCatalog: ExternalCatalog
    = if (SparkSession.active.conf.get("spark.sql.test.env").equalsIgnoreCase("true")) {
      new FSMetaStoreCatalog(
        catName,
        sparkConf = SparkSession.active.sharedState.conf,
        hadoopConfig = SparkSession.active.sharedState.hadoopConf
      )
    } else {
      new HMSCatalog(
        catName,
        sparkConf = SparkSession.active.sharedState.conf,
        hadoopConfig = SparkSession.active.sharedState.hadoopConf
      )
    }

    if(exists){
      val ct = externalCatalog.getTable(dbName,tableName)
      val preparedTable = prepareTable(sparkSession, plan, ct)
      externalCatalog.alterTable(preparedTable)
      Seq.empty[Row]
    }else{
      throw new IllegalArgumentException("View does not exists")
    }

  }

  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan, oldTable:CatalogTable): LogicalPlan = {
    val userSpecifiedColumns = analyzedPlan.schema.map(f=> (f.name, f.getComment())).toSeq
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }



  private def prepareTable(session: SparkSession, analyzedPlan: LogicalPlan, oldTable: CatalogTable): CatalogTable = {
    if (originalText.isEmpty) {
      throw QueryCompilationErrors.createPersistedViewFromDatasetAPINotAllowedError()
    }
    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session,analyzedPlan,oldTable).schema, session.sessionState.conf)
    val newProperties = oldTable.properties

    CatalogTable(

      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasedSchema,
      properties = newProperties,
      viewOriginalText = originalText,
      viewText = originalText,
      comment = oldTable.comment
    )
  }

}

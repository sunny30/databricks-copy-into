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
import org.apache.spark.sql.execution.command.{CreateViewCommand, RunnableCommand}
import org.apache.spark.sql.hive.catalog.{FSMetaStoreCatalog, HMSCatalog}
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
  extends RunnableCommand with AnalysisOnlyCommand {


  override protected def withNewChildrenInternal(
                                                  newChildren: IndexedSeq[LogicalPlan]): NonDefaultCatalogCreateViewCommand = {
    assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  // `plan` needs to be analyzed, but shouldn't be optimized so that caching works correctly.
  override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

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
    if(!exists){
      externalCatalog.createTable(prepareTable(sparkSession, plan),false)
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
    val aliasedSchema = CharVarcharUtils.getRawSchema(
      aliasPlan(session, analyzedPlan).schema, session.sessionState.conf)
    val newProperties = generateViewProperties(
      properties, session, analyzedPlan, aliasedSchema.fieldNames)

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

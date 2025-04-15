package org.apache.spark.sql.hive.plan.spark.sql.parser

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, PersistedView, UnresolvedIdentifier, UnresolvedNamespace, UnresolvedTableOrView, UnresolvedView}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.{AlterViewQueryContext, CreateViewContext, IdentifierReferenceContext, RenameTableContext}
import org.apache.spark.sql.catalyst.plans.logical.{CreateView, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.PARAMETER
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hive.plan.spark.sql.execution.views.ddl.{RenameCatalogView, ShowCatalogViews}
import org.apache.spark.sql.hive.plan.spark.sql.execution.{NonDefaultCatalogAlterViewQueryCommand, NonDefaultCatalogCreateViewCommand, NonDefaultCatalogDropViewCommand}

import scala.collection.JavaConverters.asScalaBufferConverter

class CustomAstBuilder extends SparkSqlAstBuilder{

  import org.apache.spark.sql.catalyst.parser.ParserUtils._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def visitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): LogicalPlan = super.visitSingleStatement(ctx)


  override def visitCreateView(ctx: CreateViewContext): LogicalPlan = withOrigin(ctx) {
    if (!ctx.identifierList.isEmpty) {
      operationNotAllowed("CREATE VIEW ... PARTITIONED ON", ctx)
    }

    checkDuplicateClauses(ctx.commentSpec(), "COMMENT", ctx)
    checkDuplicateClauses(ctx.PARTITIONED, "PARTITIONED ON", ctx)
    checkDuplicateClauses(ctx.TBLPROPERTIES, "TBLPROPERTIES", ctx)

    val userSpecifiedColumns = Option(ctx.identifierCommentList).toSeq.flatMap { icl =>
      icl.identifierComment.asScala.map { ic =>
        ic.identifier.getText -> Option(ic.commentSpec()).map(visitCommentSpec)
      }
    }

    if (ctx.EXISTS != null && ctx.REPLACE != null) {
      throw QueryParsingErrors.createViewWithBothIfNotExistsAndReplaceError(ctx)
    }

    val properties = ctx.propertyList.asScala.headOption.map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
    if (ctx.TEMPORARY != null && !properties.isEmpty) {
      operationNotAllowed("TBLPROPERTIES can't coexist with CREATE TEMPORARY VIEW", ctx)
    }

    val viewType = if (ctx.TEMPORARY == null) {
      PersistedView
    } else if (ctx.GLOBAL != null) {
      GlobalTempView
    } else {
      LocalTempView
    }
    val qPlan: LogicalPlan = plan(ctx.query)

    // Disallow parameter markers in the body of the view.
    // We need this limitation because we store the original query text, pre substitution.
    // To lift this we would need to reconstitute the body with parameter markers replaced with the
    // values given at CREATE VIEW time, or we would need to store the parameter values alongside
    // the text.
    //checkInvalidParameter(qPlan, "CREATE VIEW body")
    if (viewType == PersistedView) {
      val originalText = source(ctx.query)
      assert(Option(originalText).isDefined,
        "'originalText' must be provided to create permanent view")

      val nameParts = withIdentClause(ctx.identifierReference(),
        UnresolvedIdentifier(_)).
        asInstanceOf[UnresolvedIdentifier].nameParts

      if(nameParts.size==3){
        NonDefaultCatalogCreateViewCommand(
          TableIdentifier(table = nameParts(2), database = Some(nameParts(1)), catalog = Some(nameParts(0))),
          userSpecifiedColumns,
          visitCommentSpecList(ctx.commentSpec()),
          properties,
          Option(source(ctx.query)),
          qPlan,
          ctx.EXISTS != null,
          ctx.REPLACE != null,
          viewType = viewType
        )

      }else {
        CreateView(
          withIdentClause(ctx.identifierReference(), UnresolvedIdentifier(_)),
          userSpecifiedColumns,
          visitCommentSpecList(ctx.commentSpec()),
          properties,
          Some(originalText),
          qPlan,
          ctx.EXISTS != null,
          ctx.REPLACE != null)
      }
    } else {
      // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with
      // 'CREATE TEMPORARY TABLE'
      if (ctx.EXISTS != null) {
        throw QueryParsingErrors.defineTempViewWithIfNotExistsError(ctx)
      }

      withIdentClause(ctx.identifierReference(), ident => {
        val tableIdentifier = ident.asTableIdentifier
        if (tableIdentifier.database.isDefined) {
          // Temporary view names should NOT contain database prefix like "database.table"
          throw QueryParsingErrors
            .notAllowedToAddDBPrefixForTempViewError(tableIdentifier.nameParts, ctx)
        }

        CreateViewCommand(
          tableIdentifier,
          userSpecifiedColumns,
          visitCommentSpecList(ctx.commentSpec()),
          properties,
          Option(source(ctx.query)),
          qPlan,
          ctx.EXISTS != null,
          ctx.REPLACE != null,
          viewType = viewType)
      })
    }
  }

  override def visitDropView(ctx: SqlBaseParser.DropViewContext): AnyRef = withOrigin (ctx) {

    val nameParts = withIdentClause(ctx.identifierReference(),
      UnresolvedIdentifier(_)).
      asInstanceOf[UnresolvedIdentifier].nameParts
    if(nameParts.size == 3){
      val catalogName = nameParts(0)
      val dbName = nameParts(1)
      val tableName = nameParts(2)
      val isExists = ctx.EXISTS !=null
      NonDefaultCatalogDropViewCommand(catalogName, dbName, tableName, isExists)
    }else {
      DropView(
        withIdentClause(ctx.identifierReference, UnresolvedIdentifier(_, allowTemp = true)),
        ctx.EXISTS != null)
    }
  }

  override def visitRenameTable(ctx: RenameTableContext): LogicalPlan = withOrigin(ctx) {
    val isView = ctx.VIEW != null
    val relationStr = if (isView) "VIEW" else "TABLE"
    if(ctx.from.multipartIdentifier().parts.size()==3 && isView){
      RenameCatalogView(
        createUnresolvedTableOrView(ctx.from, s"ALTER $relationStr ... RENAME TO"),
        visitMultipartIdentifier(ctx.to),
        isView
      )
    }else {
      RenameTable(
        createUnresolvedTableOrView(ctx.from, s"ALTER $relationStr ... RENAME TO"),
        visitMultipartIdentifier(ctx.to),
        isView)
    }
  }

  private def createUnresolvedTableOrView(
                                           ctx: IdentifierReferenceContext,
                                           commandName: String,
                                           allowTempView: Boolean = true): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx, UnresolvedTableOrView(_, commandName, allowTempView))
  }

  override def visitShowViews(ctx: SqlBaseParser.ShowViewsContext): LogicalPlan = {
    val ns = if (ctx.identifierReference() != null) {
      withIdentClause(ctx.identifierReference, UnresolvedNamespace(_))
    } else {
      UnresolvedNamespace(Seq.empty[String])
    }
    if(ctx.identifierReference.multipartIdentifier().parts.size()==2){
      ShowCatalogViews(ns, Option(ctx.pattern).map(x => string(visitStringLit(x))))
    }else {
      ShowViews(ns, Option(ctx.pattern).map(x => string(visitStringLit(x))))
    }
  }

  override def visitAlterViewQuery(ctx: AlterViewQueryContext): LogicalPlan = withOrigin(ctx) {
    if(ctx.identifierReference().multipartIdentifier().parts.size() == 3){

      val nameParts = withIdentClause(ctx.identifierReference(),
        UnresolvedIdentifier(_)).
        asInstanceOf[UnresolvedIdentifier].nameParts
      val catalogName = nameParts(0)
      val dbName = nameParts(1)
      val tableName = nameParts(2)
      val orignalText =   source(ctx.query)
      val query = plan(ctx.query)

      NonDefaultCatalogAlterViewQueryCommand(
        TableIdentifier(table = nameParts(2), database = Some(nameParts(1)), catalog = Some(nameParts(0))),
        originalText = Some(orignalText),
        plan = query
      )

    }else {
      AlterViewAs(
        createUnresolvedView(ctx.identifierReference, "ALTER VIEW ... AS"),
        originalText = source(ctx.query),
        query = plan(ctx.query))
    }
  }

  private def createUnresolvedView(
                                    ctx: IdentifierReferenceContext,
                                    commandName: String,
                                    allowTemp: Boolean = true,
                                    relationTypeMismatchHint: Option[String] = None): LogicalPlan = withOrigin(ctx) {
    withIdentClause(ctx, UnresolvedView(_, commandName, allowTemp, relationTypeMismatchHint))
  }

//  private def checkInvalidParameter(plan: LogicalPlan, statement: String):
//  Unit = {
//    plan.foreach { p =>
//      p.expressions.foreach { expr =>
//        if (expr.containsPattern(PARAMETER)) {
//          throw QueryParsingErrors.parameterMarkerNotAllowed(statement, p.origin)
//        }
//      }
//    }
//    plan.children.foreach(p => checkInvalidParameter(p, statement))
//    plan.innerChildren.collect {
//      case child: LogicalPlan => checkInvalidParameter(child, statement)
//    }
//  }
}

package org.apache.spark.sql.hive.plan.spark.sql.parser

import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView, PersistedView, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateViewContext
import org.apache.spark.sql.catalyst.plans.logical.{CreateView, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.PARAMETER
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.execution.command.CreateViewCommand

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
    checkInvalidParameter(qPlan, "CREATE VIEW body")
    if (viewType == PersistedView) {
      val originalText = source(ctx.query)
      assert(Option(originalText).isDefined,
        "'originalText' must be provided to create permanent view")
      CreateView(
        withIdentClause(ctx.identifierReference(), UnresolvedIdentifier(_)),
        userSpecifiedColumns,
        visitCommentSpecList(ctx.commentSpec()),
        properties,
        Some(originalText),
        qPlan,
        ctx.EXISTS != null,
        ctx.REPLACE != null)
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

  private def checkInvalidParameter(plan: LogicalPlan, statement: String):
  Unit = {
    plan.foreach { p =>
      p.expressions.foreach { expr =>
        if (expr.containsPattern(PARAMETER)) {
          throw QueryParsingErrors.parameterMarkerNotAllowed(statement, p.origin)
        }
      }
    }
    plan.children.foreach(p => checkInvalidParameter(p, statement))
    plan.innerChildren.collect {
      case child: LogicalPlan => checkInvalidParameter(child, statement)
    }
  }
}

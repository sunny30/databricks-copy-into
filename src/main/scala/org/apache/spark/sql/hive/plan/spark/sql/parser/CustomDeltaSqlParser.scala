package org.apache.spark.sql.hive.plan.spark.sql.parser

import io.delta.sql.parser.{DeltaSqlAstBuilder, DeltaSqlBaseLexer, DeltaSqlBaseParser, UpperCaseCharStream}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface, PostProcessor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.delta.skipping.clustering.temp.{ClusterByParserUtils, ClusterByPlan}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser

class CustomDeltaSqlParser(val delegate: CustomSparkSQLParser) extends ParserInterface {

  private val builder = new CustomDeltaSqlAstBuilder()
  private val substitution = new VariableSubstitution


  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    builder.visit(parser.singleStatement()) match {
      case clusterByPlan: ClusterByPlan =>
        ClusterByParserUtils(clusterByPlan, new SparkSqlParser).parsePlan(sqlText)
      case plan: LogicalPlan => plan
      case _ =>
        try {
          delegate.parserSparkSQLPlan(sqlText)
        }catch {
          case e:Exception => new HudiIcebergParser(delegate).parsePlan(sqlText)
        }
    }
  }


   override def parseQuery(sqlText: String): LogicalPlan = delegate.parseQuery(sqlText)

    // scalastyle:off line.size.limit

    /**
     * Fork from `org.apache.spark.sql.catalyst.parser.AbstractSqlParser#parse(java.lang.String, scala.Function1)`.
     *
     * @see https://github.com/apache/spark/blob/v2.4.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala#L81
     */
    // scalastyle:on
    protected def parse[T](command: String)(toResult: DeltaSqlBaseParser => T): T = {
      val lexer = new DeltaSqlBaseLexer(
        new UpperCaseCharStream(CharStreams.fromString(substitution.substitute(command))))
      lexer.removeErrorListeners()
      lexer.addErrorListener(ParseErrorListener)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new DeltaSqlBaseParser(tokenStream)
      parser.addParseListener(PostProcessor)
      parser.removeErrorListeners()
      parser.addErrorListener(ParseErrorListener)

      try {
        try {
          // first, try parsing with potentially faster SLL mode
          parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
          toResult(parser)
        } catch {
          case e: ParseCancellationException =>
            // if we fail, parse with LL mode
            tokenStream.seek(0) // rewind input stream
            parser.reset()

            // Try Again.
            parser.getInterpreter.setPredictionMode(PredictionMode.LL)
            toResult(parser)
        }
      } catch {
        case e: ParseException if e.command.isDefined =>
          throw e
        case e: ParseException =>
          throw e.withCommand(command)
        case e: AnalysisException =>
          val position = Origin(e.line, e.startPosition)
          throw new ParseException(
            command = Option(command),
            start = position,
            stop = position,
            errorClass = "DELTA_PARSING_ANALYSIS_ERROR",
            messageParameters = Map("msg" -> e.message))
      }
    }

    override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

    override def parseTableIdentifier(sqlText: String): TableIdentifier =
      delegate.parseTableIdentifier(sqlText)

    override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
      delegate.parseFunctionIdentifier(sqlText)

    override def parseMultipartIdentifier(sqlText: String): Seq[String] =
      delegate.parseMultipartIdentifier(sqlText)

    override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

    override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)



}

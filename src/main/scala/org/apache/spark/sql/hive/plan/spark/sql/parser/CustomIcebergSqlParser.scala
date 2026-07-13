package org.apache.spark.sql.hive.plan.spark.sql.parser
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{BailErrorStrategy, CharStreams, CommonTokenStream, DefaultErrorStrategy}
import org.apache.iceberg.common.DynConstructors
import org.apache.iceberg.spark.ExtendedParser
import org.apache.iceberg.spark.ExtendedParser.RawOrderField
import org.apache.iceberg.spark.procedures.SparkProcedures
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.RewriteViewCommands
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.{IcebergParseErrorListener, IcebergParseException, IcebergSqlExtensionsAstBuilder, IcebergSqlExtensionsLexer, IcebergSqlExtensionsParser, IcebergSqlExtensionsPostProcessor, UpperCaseCharStream}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.{DataType, StructType}

import java.util
import java.util.Locale
import scala.jdk.CollectionConverters.{asScalaSetConverter, seqAsJavaListConverter}

class CustomIcebergSqlParser(val delegate: CustomSparkSQLParser)  extends ParserInterface with ExtendedParser {

  import CustomIcebergSqlParser._

  private lazy val substitutor = substitutorCtor.newInstance(SQLConf.get)
  private lazy val astBuilder = new IcebergSqlExtensionsAstBuilder(delegate)

  override def parsePlan(sqlText: String): LogicalPlan = {
    parse(sqlText) { parser => astBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
  }

  def parse[T](command: String)(toResult: IcebergSqlExtensionsParser => T): T = {
    val sqlTextAfterSubstitution = substitutor.substitute(command)
    val lexer = new IcebergSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(sqlTextAfterSubstitution)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(IcebergParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new IcebergSqlExtensionsParser(tokenStream)
    parser.addParseListener(IcebergSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(IcebergParseErrorListener)

    // https://github.com/antlr/antlr4/issues/192#issuecomment-15238595
    // Save a great deal of time on correct inputs by using a two-stage parsing strategy.
    try {
      try {
        // first, try parsing with potentially faster SLL mode and BailErrorStrategy
        parser.setErrorHandler(new BailErrorStrategy)
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode with DefaultErrorStrategy
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.setErrorHandler(new DefaultErrorStrategy)
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: IcebergParseException if e.command.isDefined =>
        throw e
      case e: IcebergParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new IcebergParseException(Option(command), e.message, position, position)
    }
  }


  def isIcebergCommand(sqlText: String): Boolean = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    val normalized = sqlTextAfterSubstitution.toLowerCase(Locale.ROOT).trim()
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      // Strip backtick then `system`.`ancestors_of` changes to system.ancestors_of
      .replaceAll("`", "")
      .trim()

    (
      normalized.startsWith("alter table") && (
        normalized.contains("add partition field") ||
          normalized.contains("drop partition field") ||
          normalized.contains("replace partition field") ||
          normalized.contains("write ordered by") ||
          normalized.contains("write locally ordered by") ||
          normalized.contains("write distributed by") ||
          normalized.contains("write unordered") ||
          normalized.contains("set identifier fields") ||
          normalized.contains("drop identifier fields") ||
          isSnapshotRefDdl(normalized)))
  }

  // All builtin Iceberg procedures are under the 'system' namespace
  def isIcebergProcedure(sqlText: String): Boolean = {
    val normalized = normalize(sqlText)
    normalized.startsWith("call") &&
      SparkProcedures.names().asScala.map("system." + _).exists(normalized.contains)
  }

  private def normalize(sqlText: String): String =
    substitutor.substitute(sqlText)
      .toLowerCase(Locale.ROOT).trim()
      .replaceAll("--.*?\\n", " ")
      .replaceAll("\\s+", " ")
      .replaceAll("/\\*.*?\\*/", " ")
      .replaceAll("`", "")
      .trim()

  private def isSnapshotRefDdl(normalized: String): Boolean = {
    normalized.contains("create branch") ||
      normalized.contains("replace branch") ||
      normalized.contains("create tag") ||
      normalized.contains("replace tag") ||
      normalized.contains("drop branch") ||
      normalized.contains("drop tag")
  }

  override def parseExpression(sqlText: String): Expression = {delegate.parseExpression(sqlText)}

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {delegate.parseTableIdentifier(sqlText)}

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {delegate.parseFunctionIdentifier(sqlText)}

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {delegate.parseMultipartIdentifier(sqlText)}

  override def parseQuery(sqlText: String): LogicalPlan = {parsePlan(sqlText)}

  override def parseSortOrder(sqlText: String): util.List[ExtendedParser.RawOrderField] = {
    val fields = parse(sqlText) { parser => astBuilder.visitSingleOrder(parser.singleOrder()) }
    fields.map { field =>
      val (term, direction, order) = field
      new RawOrderField(term, direction, order)
    }.asJava
  }

  override def parseTableSchema(sqlText: String): StructType = {delegate.parseTableSchema(sqlText)}

  override def parseDataType(sqlText: String): DataType = {delegate.parseDataType(sqlText)}
}

object CustomIcebergSqlParser {
  private val substitutorCtor: DynConstructors.Ctor[VariableSubstitution] =
    DynConstructors.builder()
      .impl(classOf[VariableSubstitution])
      .impl(classOf[VariableSubstitution], classOf[SQLConf])
      .build()
}


package org.apache.spark.sql.hive.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.hive.plan.{CopyIntoFromFilesCommand, CopyIntoFromLocationCommand, CopyIntoFromSelectClauseCommand, GenerateDeltaLogCommand}

class CustomSqlParser(val parserInterface: ParserInterface) extends AbstractCustomSqlParser(parserInterface = parserInterface) {

  val SHOW = Keyword("show")
  val CATALOGS = Keyword("catalogs")
  val GENERATE = Keyword("generate")
  val DELTALOG = Keyword("deltalog")
  val FOR = Keyword("for")
  val TABLE = Keyword("table")
  val LOCATION = Keyword("location")
  val USING = Keyword("using")
  val COPY = Keyword("copy")
  val INTO = Keyword("into")
  val FROM = Keyword("from")
  val FILEFORMAT = Keyword("fileformat")
  val FILES = Keyword("files")
  def FORMATOPTIONS:Parser[String] = "format_options"
  def COPYOPTIONS:Parser[String] = "copy_options"
  def openParen: Parser[String] = "("
  def closeParen: Parser[String] = ")"
  def quoteValue:Parser[String] = """\'"""




  def dash: Parser[String] = "-"

  def underscore: Parser[String] = "_"

  def dot: Parser[String] = "."

  override def parse(input: String): LogicalPlan = super.parse(input)

  override protected def start: Parser[LogicalPlan] = rule1 | rule2 | copy_into_location_rule3
    copy_into_location_rule1 | copy_into_location_rule2


  def isValidCharacterInsideQuote(c: Char): Boolean = {
    val firstCriterion = true
    val secondCriterion = '''.equals(c)
    firstCriterion && !secondCriterion
  }

  def isValidCharacterInsideProjectParen(c: Char): Boolean = {
    val firstCriterion = true
    val secondCriterion = '}'.equals(c)
    firstCriterion && !secondCriterion
  }

  def quoteIdent: Parser[String] =
    "" ~> // handle whitespace
      rep1(acceptIf(ch => isValidCharacterInsideQuote(ch))("identifier expected but '" + _ + "' found"),
        elem("identifier part", isValidCharacterInsideQuote(_: Char))) ^^ (_.mkString)


  def projectParenClause:Parser[String] = "{" ~> rep1(acceptIf(ch => isValidCharacterInsideProjectParen(ch))("identifier expected but '" + _ + "' found"),
    elem("identifier part", isValidCharacterInsideProjectParen(_: Char))) ^^ (_.mkString)


  def singleQuote = "'"
  def parseLocation: Parser[String] = singleQuote~>quoteIdent<~singleQuote^^{
    case l => l
  }


  def nonJavaident: Parser[String] =
    "" ~> // handle whitespace
      rep1(acceptIf(Character.isLetterOrDigit)("identifier expected but '" + _ + "' found"),
        elem("identifier part", Character.isLetterOrDigit(_: Char))) ^^ (_.mkString)


  def sqlOptionalPart: Parser[String] = {
    (dash | underscore) ~ nonJavaident ^^ {
      case s ~ id => s + id
    }
  }

  def sqlOptionalRepetativePart: Parser[String] = {
    rep(sqlOptionalPart) ^^ {
      case p => p.mkString
    }
  }

  def sqlIdentifier: Parser[String] = {
    ident ~ opt(sqlOptionalRepetativePart) ^^ {
      case i ~ s => {
        if (s.isEmpty)
          i
        else
          i + s.get
      }
    }
  }

  def parseTable: Parser[(String, String)] = {
    sqlIdentifier ~ dot ~ (sqlIdentifier) ^^ {
      case d ~ _ ~ t => (d, t)
    }
  }

  def parseEqual: Parser[String] = "="

  def parseFormat: Parser[String] = {
    FILEFORMAT~parseEqual~sqlIdentifier^^{
      case _~_~format => format
    }
  }

  def quote: Parser[String] = "'"

  def parseFormatOptions:Parser[Seq[(String,String)]]={
    FORMATOPTIONS~openParen~>rep1sep(parseSingleProperty,",")<~closeParen^^{
      case props=> props
    }
  }

  def parseCopyOptions:Parser[Seq[(String,String)]]={
    COPYOPTIONS~openParen~>rep1sep(parseSingleProperty,",")<~closeParen^^{
      case props=> props
    }
  }

  def properties: Parser[String] = "properties"

  def parseSingleProperty: Parser[(String, String)] = {
    parseKey ~ parseEqual ~ parseValue ^^ {
      case key ~ _ ~ value => (key, value)
    }
  }

  def parseKey: Parser[String] = {
    quote ~> keyIdent <~ quote ^^ {
      case key => key
    }
  }

  def keyIdent: Parser[String] = {
    "" ~>
      rep1(
        acceptIf(x => isKeyCharacterValue(x))("identifier expected but '" + _ + "' found"),
        elem("identifier part", isKeyCharacterValue(_: Char))) ^^ (_.mkString)

  }

  def isKeyCharacterValue(c: Char): Boolean = {
    Character.isLetterOrDigit(c) || '.'.equals(c) || '_'.equals(c)
  }

  def parseValue: Parser[String] = {
    nonJavaident | (quote ~> (quoteValue| quoteIdent | quote) <~ quote) ^^ {
      case value => value
    }
  }

  def parseSingleFile: Parser[(String)] = {
    singleQuote~>quoteIdent<~singleQuote^^{
      case l => l
    }
  }

  def parseFilePaths: Parser[Seq[(String)]]={
    openParen~>rep1sep(parseSingleFile, ",")<~closeParen^^{
      case props=> props
    }
  }

  def parseFiles: Parser[Seq[String]] = {
    FILES~parseEqual~parseFilePaths^^{
      case _~_~files => files
    }
  }


  def rule1: Parser[LogicalPlan] = GENERATE ~ DELTALOG ~ FOR ~ TABLE ~ parseTable ~ USING ~ ident ^^ {
    case _ ~ _ ~ _ ~ _ ~ t ~ _ ~ f => {
      val ct = SparkSession.active.sessionState.catalog.getTableMetadata(TableIdentifier(t._2, Some(t._1)))
      GenerateDeltaLogCommand(Some(ct), None, f)
    }
  }

  def rule2: Parser[LogicalPlan] = GENERATE ~ DELTALOG ~ FOR ~ LOCATION ~ parseLocation ~ USING ~ ident ^^ {
    case _ ~ _ ~ _ ~ _ ~ loc ~ _ ~ f => {
      GenerateDeltaLogCommand(None, Some(loc), f)
    }
  }

  def copy_into_location_rule1: Parser[LogicalPlan] = COPY~INTO~parseTable~FROM~parseLocation~parseFormat^^{
    case _ ~ _ ~ newTable ~ _ ~ loc ~ fm => CopyIntoFromLocationCommand(
      databaseName = newTable._1,
      newTableName = newTable._2,
      fromLocation = loc,
      format = fm
    )
  }

  def copy_into_location_rule2: Parser[LogicalPlan] = COPY ~ INTO ~ parseTable ~ FROM ~ projectParenClause ~ parseFormat ^^ {
    case _ ~ _ ~ newTable ~ _ ~ prj_loc ~ fm =>
      val prjClause = prj_loc.split("from ")(0)
      val loc = prj_loc.split("from ")(1).trim

      CopyIntoFromSelectClauseCommand(
      databaseName = newTable._1,
      newTableName = newTable._2,
      fromLocation = loc,
      format = fm, selectClause = prjClause
    )
  }

  def copy_into_location_rule3: Parser[LogicalPlan] = COPY~INTO~parseTable~FROM~parseLocation~parseFormat~opt(parseFiles)~opt(parseFormatOptions)~opt(parseCopyOptions) ^^ {
    case _ ~ _ ~ newTable ~ _ ~ loc ~ fm ~ files ~ formatOptions ~ copyOptions =>

      CopyIntoFromFilesCommand(
        databaseName = newTable._1,
        newTableName = newTable._2,
        fromLocation = loc,
        format = fm,
        files = files.getOrElse(Seq.empty[String]),
        formatOptions = Option.apply(formatOptions.getOrElse(Seq.empty[(String, String)]).toMap),
        Option.apply(copyOptions.getOrElse(Seq.empty[(String, String)]).toMap)
      )
  }
}

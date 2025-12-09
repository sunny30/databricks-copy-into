package org.apache.spark.sql.hive.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.execution.command.SetCatalogCommand
import org.apache.spark.sql.hive.plan.{CopyIntoFromFilesCommand, CopyIntoFromLocationCommand, CopyIntoFromSelectClauseCommand, CreateRowSecFunction, GenerateDeltaLogCommand, GrantRowFunc, RefreshCatalogEntity}

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
  val PATTERN = Keyword("pattern")

  val REFRESH = Keyword("REFRESH")
  val CATALOG = Keyword("CATALOG")
  val SCHEMA = Keyword("SCHEMA")
  val EXTERNAL = Keyword("EXTERNAL")
  val IN = Keyword("IN")
  val USE = Keyword("USE")
  def FORMATOPTIONS:Parser[String] = "format_options"
  def COPYOPTIONS:Parser[String] = "copy_options"
  def openParen: Parser[String] = "("
  def closeParen: Parser[String] = ")"
  def quoteValue:Parser[String] = """\'"""

  def createL: Parser[String] = "create"
  def createU: Parser[String] = "CREATE"

  def create:Parser[String] = createL | createU

  def functionL:Parser[String] = "function"
  def functionU:Parser[String] = "FUNCTION"

  def function:Parser[String] = functionL | functionU

  def forL: Parser[String] = "for"
  def forU: Parser[String] = "FOR"

  def forX:Parser[String] = forL | forU

  def tableL: Parser[String] = "table"
  def tableU: Parser[String] = "TABLE"
  def table:Parser[String] = tableL | tableU

  def whereL: Parser[String] = "where"
  def whereU: Parser[String] = "WHERE"
  def where:Parser[String] = whereL | whereU


  def doubleQuoteValue:Parser[String] = """\""""



  def row_levelL:Parser[String] = "row_level"
  def row_levelU:Parser[String] = "ROW_LEVEL"

  def row_level:Parser[String] = row_levelL | row_levelU

  def userL:Parser[String] = "user"
  def userU:Parser[String] = "USER"
  def user:Parser[String] = userL | userU

  def grantL = "grant"
  def grantU = "GRANT"

  def grant: Parser[String] = grantL | grantU


  def dash: Parser[String] = "-"

  def underscore: Parser[String] = "_"

  def dot: Parser[String] = "."

  override def parse(input: String): LogicalPlan = super.parse(input)

  override protected def start: Parser[LogicalPlan] = rule1 | rule2 | copy_into_location_rule3 | copy_into_location_rule2 | row_level_rule1 |
    copy_into_location_rule1 | row_level_rule2 | refresh_catalog_schema | refresh_catalog_table | use_catalog


  def isValidCharacterInsideQuote(c: Char): Boolean = {
    val firstCriterion = true
    val secondCriterion = '''.equals(c)
    firstCriterion && !secondCriterion
  }

  def isValidCharacterInsideDoubleQuote(c: Char): Boolean = {
    val firstCriterion = true
    val secondCriterion = '"'.equals(c)
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


  def doubleQuoteIdent: Parser[String] =
    "" ~> // handle whitespace
      rep1(acceptIf(ch => isValidCharacterInsideDoubleQuote(ch))("identifier expected but '" + _ + "' found"),
        elem("identifier part", isValidCharacterInsideDoubleQuote(_: Char))) ^^ (_.mkString)


  def projectParenClause:Parser[String] = "{" ~> rep1(acceptIf(ch => isValidCharacterInsideProjectParen(ch))("identifier expected but '" + _ + "' found"),
    elem("identifier part", isValidCharacterInsideProjectParen(_: Char)))<~"}" ^^ (_.mkString)


  def singleQuote = "'"

  def doubleQuote = """\""""
  def parseLocation: Parser[String] = singleQuote~>quoteIdent<~singleQuote^^{
    case l => l
  }

  def parseSingleQuoteIdent = parseSingleFile

  def parseDoubleQuoteIdent = (doubleQuote~>doubleQuoteIdent<~doubleQuote)

  def parsePredicate:Parser[String] = parseDoubleQuoteIdent |  parseSingleQuoteIdent

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


  def parseCatalogTable:Parser[(String,String,String)] ={
    sqlIdentifier ~ dot ~ sqlIdentifier ~ dot ~ (sqlIdentifier) ^^{
      case c ~_ ~ db ~ _ ~ tbl => (c, db, tbl)
    }
  }

  def parseEqual: Parser[String] = "="

  def parseFormat: Parser[String] = {
    FILEFORMAT~parseEqual~sqlIdentifier^^{
      case _~_~format => format
    }
  }

  def parsePattern: Parser[String] = {
    PATTERN~parseEqual~parseSingleFile^^{
      case _~_~pattern => pattern
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

  def refreshL: Parser[String] = "refresh"

  def refreshU: Parser[String] = "REFRESH"

  def refresh: Parser[String] = refreshL | refreshU

  def parseSingleFile: Parser[(String)] = {
    singleQuote~>quoteIdent<~singleQuote^^{
      case l => l
    }
  }

  def qualifiedCatalogSchema: Parser[String] = {
    sqlIdentifier ~ dot ~ sqlIdentifier ^^ {
      case c ~ _ ~ s => c + "." + s
    }
  }

  def qualifiecCatalogTable: Parser[String] = {
    sqlIdentifier ~ dot ~ sqlIdentifier ~ dot ~ sqlIdentifier ^^ {
      case c ~ _ ~ s ~ _ ~ t => c + "." + s + "." + t
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


  def refresh_catalog_schema:Parser[LogicalPlan] = refresh ~ SCHEMA ~ IN ~ EXTERNAL ~ CATALOG ~ qualifiedCatalogSchema ^^ {
    case _ ~ _ ~ _ ~ _ ~ _ ~ qs => RefreshCatalogEntity(qs)
  }

  def refresh_catalog_table:Parser[LogicalPlan] = refresh ~ TABLE ~ IN ~ EXTERNAL ~ CATALOG ~ qualifiecCatalogTable ^^ {
    case _ ~ _ ~ _ ~ _ ~ _ ~ qs => RefreshCatalogEntity(qs)
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

  def row_level_rule1: Parser[LogicalPlan] = create~function~sqlIdentifier~forX~table~parseCatalogTable~where~parsePredicate^^{
    case _ ~ _ ~ func_name ~ _ ~ _ ~ tbl_tup~ _~cond => CreateRowSecFunction(tbl_tup,cond,func_name)
  }

  def row_level_rule2:Parser[LogicalPlan] = grant ~ row_level ~ sqlIdentifier~forX~user~sqlIdentifier^^{
    case _~_~func_name~_~_~usr => GrantRowFunc(funcName = func_name, user = usr)
  }
  def copy_into_location_rule2: Parser[LogicalPlan] = COPY ~ INTO ~ parseTable ~ FROM ~ projectParenClause ~ parseFormat~opt(parsePattern)~opt(parseFiles)~opt(parseFormatOptions)~opt(parseCopyOptions) ^^ {
    case _ ~ _ ~ newTable ~ _ ~ prj_loc ~ fm~ pattern ~ files ~ formatOptions ~ copyOptions =>
      val prjClause = prj_loc.split("from")(0)
      val loc = prj_loc.split("from ")(1).replaceAll("'","").replaceAll(" ","")

      CopyIntoFromSelectClauseCommand(
        databaseName = newTable._1,
        newTableName = newTable._2,
        fromLocation = loc,
        format = fm,
        selectClause = prjClause,
        pattern = pattern,
        files = files.getOrElse(Seq.empty[String]),
        formatOptions = Option.apply(formatOptions.getOrElse(Seq.empty[(String, String)]).toMap),
        copyOptionsMap = Option.apply(copyOptions.getOrElse(Seq.empty[(String, String)]).toMap)

      )
  }

  def copy_into_location_rule3: Parser[LogicalPlan] = COPY~INTO~parseTable~FROM~parseLocation~parseFormat~opt(parsePattern)~opt(parseFiles)~opt(parseFormatOptions)~opt(parseCopyOptions) ^^ {
    case _ ~ _ ~ newTable ~ _ ~ loc ~ fm ~ pattern ~ files ~ formatOptions ~ copyOptions =>

      CopyIntoFromFilesCommand(
        databaseName = newTable._1,
        newTableName = newTable._2,
        fromLocation = loc,
        format = fm,
        pattern = pattern,
        files = files.getOrElse(Seq.empty[String]),
        formatOptions = Option.apply(formatOptions.getOrElse(Seq.empty[(String, String)]).toMap),
        Option.apply(copyOptions.getOrElse(Seq.empty[(String, String)]).toMap),
      )
  }


  def use_catalog = USE ~ CATALOG ~ sqlIdentifier ^^ {
    case _ ~ _ ~ catalog_name => SetCatalogCommand(catalog_name)
  }
}

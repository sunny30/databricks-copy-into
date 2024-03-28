package org.apache.spark.sql.hive.parser

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.hive.plan.{CopyIntoFromLocationCommand, CopyIntoFromSelectClauseCommand, GenerateDeltaLogCommand}

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




  def dash: Parser[String] = "-"

  def underscore: Parser[String] = "_"

  def dot: Parser[String] = "."

  override def parse(input: String): LogicalPlan = super.parse(input)

  override protected def start: Parser[LogicalPlan] = rule1 | rule2 |
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


  def rule1: Parser[LogicalPlan] = GENERATE ~ DELTALOG ~ FOR ~ TABLE ~ parseTable ~ USING ~ ident ^^ {
    case _ ~ _ ~ _ ~ _ ~ t ~ _ ~ f => {
      val ct = SparkSession.active.sessionState.catalog.getTableMetadata(TableIdentifier(t._2, Some(t._1)))
      GenerateDeltaLogCommand(Some(ct), None, f)
    }
  }

  def rule2: Parser[LogicalPlan] = GENERATE ~ DELTALOG ~ FOR ~ LOCATION ~ quoteIdent ~ USING ~ ident ^^ {
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
}

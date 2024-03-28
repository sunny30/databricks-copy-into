package org.apache.spark.sql.hive.parser

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}
import scala.util.parsing.input.CharArrayReader.EofCh

abstract class AbstractCustomSqlParser(parserInterface: ParserInterface) extends JavaTokenParsers with PackratParsers {


  def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    parseAll(start, input) match {
      case Success(plan, _) => plan
      case failureOrError => parserInterface.parsePlan(input)
    }
  }

  /* One time initialization of lexical.This avoid reinitialization of  lexical in parse method */
  protected lazy val initLexical: Unit = lexical.initialize(reservedWords)

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)

    def parser: Parser[String] = normalize
  }


  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  protected lazy val reservedWords: Seq[String] =
  this
    .getClass
    .getMethods
    .filter(_.getReturnType == classOf[Keyword])
    .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  // Set the keywords as empty by default, will change that later.
  val lexical = new CmdsLexical

  protected def start: Parser[LogicalPlan]

}

class CmdsLexical extends StdLexical {
  case class DecimalLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string */
  def normalizeKeyword(str: String): String = str.toLowerCase

  delimiters += (
    "@", "*", "+", "<", "=", "<>", "!=", "<=", ">=", ">", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    (rep1(digit) ~ scientificNotation ^^ { case i ~ s => DecimalLit(i.mkString + s) }
      | '.' ~> (rep1(digit) ~ scientificNotation) ^^ { case i ~ s => DecimalLit("0." + i.mkString + s) }
      | rep1(digit) ~ ('.' ~> digit.*) ~ scientificNotation ^^ { case i1 ~ i2 ~ s => DecimalLit(i1.mkString + "." + i2.mkString + s) }
      | digit.* ~ identChar ~ (identChar | digit).* ^^ { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
      | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
      case i ~ None => NumericLit(i.mkString)
      case i ~ Some(d) => DecimalLit(i.mkString + "." + d.mkString)
    }
      | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^ { case chars => StringLit(chars mkString "") }
      | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^ { case chars => StringLit(chars mkString "") }
      | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^ { case chars => Identifier(chars mkString "") }
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )

  override def identChar: Parser[Elem] = letter | elem('_')

  private lazy val scientificNotation: Parser[String] =
    (elem('e') | elem('E')) ~> (elem('+') | elem('-')).? ~ rep1(digit) ^^ {
      case s ~ rest => "e" + s.mkString + rest.mkString
    }

  //  override def whitespace: Parser[Any] =
  //    ( whitespaceChar
  //      | '/' ~ '*' ~ comment
  //      | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
  //      | '#' ~ chrExcept(EofCh, '\n').*
  //      | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
  //      | '/' ~ '*' ~ failure("unclosed comment")
  //      ).*
}




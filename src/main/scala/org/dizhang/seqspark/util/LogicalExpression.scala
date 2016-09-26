package org.dizhang.seqspark.util

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * A logical expression parser
  */

case class LogicalExpression(varMap: Map[String, String]) extends JavaTokenParsers {
  def expr: Parser[Boolean] = term~rep("or"~term) ^^ {
    case f1 ~ f2 => (f1 /: f2)(_ || _._2)
  }
  def term: Parser[Boolean] = factor~rep("and"~factor) ^^ {
    case f1 ~ f2 => (f1 /: f2)(_ && _._2)
  }
  def factor: Parser[Boolean] = comparison | ("("~expr~")" ^^ { case "("~x~")" => x })
  def comparison: Parser[Boolean] = stringComparison | numberComparison | existence
  def stringComparison: Parser[Boolean] =
    """[a-zA-Z_]\w*""".r~("=="|"!=")~stringLiteral ^^ {
      case name~"=="~value => varMap(name) == value.substring(1,value.length - 1)
      case name~"!="~value => varMap(name) != value.substring(1,value.length - 1)
    }
  def numberComparison: Parser[Boolean] =
    """[a-zA-Z_]\w*""".r~(">="|"<="|"=="|"!="|">"|"<")~floatingPointNumber ^^ {
      case name~">"~value => varMap(name).toDouble > value.toDouble
      case name~">="~value => varMap(name).toDouble >= value.toDouble
      case name~"<"~value => varMap(name).toDouble < value.toDouble
      case name~"<="~value => varMap(name).toDouble <= value.toDouble
      case name~"=="~value => varMap(name).toDouble == value.toDouble
      case name~"!="~value => varMap(name).toDouble != value.toDouble
    }
  def existence: Parser[Boolean] =
    """[a-zA-Z_]\w*""".r ^^ (name => varMap(name) == "1")
  def parse(input: String) = this.parseAll(expr, input)
  def judge(input: String) = this.parse(input).get
}

object LogicalExpression {
  def judge(varMap: Map[String, String])(input: String): Boolean = {
    LogicalExpression(varMap).judge(input)
  }

  def analyze(input: String): Set[String] = {
    Analyzer.names(input)
  }

  case object Analyzer extends JavaTokenParsers {
    def expr: Parser[Set[String]] = term~rep("or"~term) ^^ {
      case f1 ~ f2 => (f1 /: f2)(_ ++ _._2)
    }
    def term: Parser[Set[String]] = factor~rep("and"~factor) ^^ {
      case f1 ~ f2 => (f1 /: f2)(_ ++ _._2)
    }
    def factor: Parser[Set[String]] = comparison | ("("~expr~")" ^^ {case "("~x~")" => x})
    def comparison: Parser[Set[String]] = stringComparison | numberComparison | existence
    def stringComparison: Parser[Set[String]] =
      """[a-zA-Z_]\w*""".r~("=="|"!=")~stringLiteral ^^ {
        case name~("=="|"!=")~value => Set(name)
      }
    def numberComparison: Parser[Set[String]] =
      """[a-zA-Z_]\w*""".r~(">="|"<="|"=="|"!="|">"|"<")~floatingPointNumber ^^ {
        case name~(">="|"<="|"=="|"!="|">"|"<")~value => Set(name)
      }
    def existence: Parser[Set[String]] =
      """[a-zA-Z_]\w*""" ^^ (name => Set(name))
    def parse(input: String) = this.parseAll(expr, input)
    def names(input: String) = this.parse(input).get
  }

}

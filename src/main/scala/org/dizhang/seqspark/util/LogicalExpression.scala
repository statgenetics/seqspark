package org.dizhang.seqspark.util


import scala.util.parsing.combinator.JavaTokenParsers
/**
  * Created by zhangdi on 9/17/16.
  */

case class LogicalExpression(varMap: Map[String, String]) extends JavaTokenParsers {
  def expr: Parser[Boolean] = term~rep("or"~term) ^^ {
    case f1 ~ f2 => (f1 /: f2)(_ || _._2)
  }
  def term: Parser[Boolean] = factor~rep("and"~factor) ^^ {
    case f1 ~ f2 => (f1 /: f2)(_ && _._2)
  }
  def factor: Parser[Boolean] = comparison | ("("~expr~")" ^^ { case "("~x~")" => x })
  def comparison: Parser[Boolean] = stringComparison | numberComparison
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
  def parse(input: String) = this.parseAll(expr, input)
  def judge(input: String) = this.parse(input).get
}

object LogicalExpression {
  def judge(varMap: Map[String, String])(input: String): Boolean = {
    LogicalExpression(varMap).judge(input)
  }
}

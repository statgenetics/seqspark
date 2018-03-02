/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.seqspark.parser

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers
import ExprToken._

class ExprLexer(keywords: mutable.HashSet[String]) extends RegexParsers {

  override def skipWhitespace = true
  override val whiteSpace = "[ \t\r\f]+".r

  def tokens: Parser[List[ExprToken]] = {
    phrase(rep1(processName | stringLit | doubleLit | intLit | booleanLit | operator | delimiter))
  }

  protected def processName: Parser[ExprToken] = {
    """[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?""".r ^^ {
      name => if (keywords.contains(name)) Keyword(name) else Identifier(name)
    }
  }

  def stringLit: Parser[StringLit] = positioned[StringLit]{
    '\'' ~  """[^']*""".r ~ '\'' ^^ {case '\'' ~ chars ~ '\'' => StringLit(chars)} |
      '\"' ~ """[^"]*""".r ~ '\"' ^^ {case '\"' ~ chars ~ '\"' => StringLit(chars)}
  }

  def intLit: Parser[IntLit] = positioned[IntLit] {
    """[+-]?\d+""".r ^^ {int => IntLit(int.toInt)}
  }

  def doubleLit: Parser[DoubleLit] = positioned[DoubleLit] {
    """[+-]?(\d+\.\d*|\d*\.\d+)([eE][+-]?\d+)?""".r  ^^ {double => DoubleLit(double.toDouble)}
  }

  def booleanLit: Parser[BooleanLit] = positioned[BooleanLit]{
    "true" ^^^ BooleanLit(true) | "false" ^^^ BooleanLit(false)
  }

  def operator: Parser[Operator] = positioned[Operator] {
    ("""(==|!=|>=|<=|[!><+\-*/]|and|or|not)""".r | "&&" | "||" ) ^^ {op => Operator(op)}
  }

  def delimiter: Parser[Delimiter] = positioned[Delimiter] {
    ("("|")"|";") ^^ {d => Delimiter(d)}
  }

  def apply(code: String): Either[LexerError, List[ExprToken]] = {
    parse(tokens, code) match {
      case NoSuccess(msg, next) => Left(LexerError(Location(next.pos.line, next.pos.column), msg))
      case Success(res, _) => Right(res)
    }
  }

}

object ExprLexer {

  def apply(keywords: List[String] = List("ifElse")): ExprLexer = {
    val kws = mutable.HashSet(keywords: _*)
    new ExprLexer(kws)
  }
}

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

class ExprLexer extends RegexParsers {

  override def skipWhitespace = true
  override val whiteSpace = "[ \t\r\f]+".r

  def tokens: Parser[List[ExprToken]] = {
    phrase(rep1(processName | lit | operator | separator))
  }

  protected def processName: Parser[ExprToken] = {
    """[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ {
      name => if (name.matches("(?i)ifelse")) IFELSE else Identifier(name)
    }
  }

  def lit: Parser[ExprToken] = stringLit | doubleLit | intLit | booleanLit

  def operator: Parser[ExprToken] = or | and | not | add | mul | equal | comp

  def separator: Parser[ExprToken] = lparen | rparen | comma | dot

  /** literals */

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


  /** logical operators */
  def or: Parser[ExprToken] = positioned[ExprToken] {
    ("||"|"(?i)or".r) ^^^ OR
  }

  def and: Parser[ExprToken] = positioned[ExprToken] {
    ("&&"|"(?i)and".r) ^^^ AND
  }

  def not: Parser[ExprToken] = positioned[ExprToken] {
    ("!"|"(?i)not".r) ^^^ NOT
  }

  /** arithmetic operators */
  def add: Parser[ExprToken] = positioned[ExprToken] {
    ("+"|"-") ^^ {op => ADD(op)}
  }

  def mul: Parser[ExprToken] =  positioned[ExprToken] {
    ("*"|"/"|"%") ^^ {op => MUL(op)}
  }

  /** comparison operators */
  def equal: Parser[ExprToken] = positioned[ExprToken] {
    ("=="|"(?i)eq".r) ^^^ EQUAL("==") | ("!="|"(?i)ne".r) ^^^ EQUAL("!=")
  }

  def comp: Parser[ExprToken] =  positioned[ExprToken] {
    """(>=|<=|[><])""".r ^^ {op => COMP(op)}
  }

  /** separators */
  def lparen: Parser[ExprToken] = positioned[ExprToken] {
    "(" ^^^ LeftParen
  }

  def rparen: Parser[ExprToken] = positioned[ExprToken] {
    ")" ^^^ RightParen
  }

  def comma: Parser[ExprToken] = positioned[ExprToken] {
    "," ^^^ Comma
  }

  def dot: Parser[ExprToken] =  positioned[ExprToken] {
    "." ^^^ Dot
  }

  def apply(code: String): Either[LexerError, List[ExprToken]] = {
    parse(tokens, code) match {
      case NoSuccess(msg, next) => Left(LexerError(Location(next.pos.line, next.pos.column), msg))
      case Success(res, _) => Right(res)
    }
  }

}

object ExprLexer {

  def apply(): ExprLexer = {
    new ExprLexer()
  }
}

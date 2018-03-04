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

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}
import scala.language.{higherKinds, existentials}
import spire.math.Number
import ExprAST._
import ExprToken._

class ExprParser extends Parsers {
  override type Elem = ExprToken

  class ExprTokenReader(tokens: Seq[ExprToken]) extends Reader[ExprToken] {
    def first: ExprToken = tokens.head
    def atEnd: Boolean = tokens.isEmpty
    def pos: Position = NoPosition
    def rest: Reader[ExprToken] = new ExprTokenReader(tokens.tail)
  }

  /** literal */
  def lit: Parser[ExprAST] = {
    accept("literal", {
      case IntLit(n) => Num(Number(n))
      case DoubleLit(d) => Num(Number(d))
      case BooleanLit(b) => Bool(b)
      case StringLit(s) => Str(s)
    })
  }

  def opTok: Parser[ExprToken] = {
    accept("operator", { case x: Operator => x })
  }

  def id: Parser[ExprToken] = {
    accept("identifier", { case x: Identifier => x })
  }

  def delimTok: Parser[ExprToken] = {
    accept("delimiter", {case x: Delimiter => x})
  }

  def expr: Parser[ExprAST]

  def simpleExpr: Parser[ExprAST] = lit | delimTok ~ expr ~ delimTok ^^ {
    case Delimiter("(") ~ e ~ Delimiter(")") => Nest(e)
  }

  def binary: Parser[ExprAST] = expr ~ opTok ~ expr ^^ {
    case e1 ~ Operator(op) ~ e2 => op match {
      case "=="|"!="|">="|"<="|">"|"<" => Comp(op, e1, e2)
      case "&&"|"and"|"And"|"AND" => And(e1, e2)
      case "||"|"or"|"Or"|"AND" => Or(e1, e2)
      case "*"|"/" => Mul(op, e1, e2)
      case "+"|"-" => Add(op, e1, e2)
    }
  }

  def unary: Parser[ExprAST] = opTok ~ simpleExpr ^^ {
    case Operator(op) ~ e => op match {
      case "!"|"not" => Not(e)
      case "+"|"-" => Sign(op, e)
    }
  }

  def variable: Parser[ExprAST] = id ^^ {
    case Identifier(n) => Variable(n)
  }

  def func: Parser[ExprAST] = id ~ lparen ~ rep(expr) ~ rparen

  def arithExpr: Parser[repr[Number]] = positioned[repr[Number]]{
    arithTerm ~ rep(Operator("+") ~ arithTerm | Operator("-") ~ arithTerm) ^^ {
      case t ~ ts => ts.foldLeft(t){
        case (a, Operator("+") ~ b) => variantAlg.numAlg.add(a, b)
        case (a, Operator("-") ~ b) => variantAlg.numAlg.sub(a, b)
      }
    }
  }

  def arithTerm: Parser[repr[Number]] = positioned[repr[Number]]{
    arithFactor ~ rep(Operator("*") ~ arithFactor | Operator("/") ~ arithFactor) ^^ {
      case t ~ ts => ts.foldLeft(t){
        case (a, Operator("*") ~ b) => variantAlg.numAlg.mul(a, b)
        case (a, Operator("/") ~ b) => variantAlg.numAlg.div(a, b)
      }
    }
  }

  def arithFactor: Parser[repr[Number]]= positioned[repr[Number]]{
    Delimiter("(") ~ arithExpr ~ Delimiter(")") ^^ {
      case Delimiter("(") ~ x ~ Delimiter(")") => x
    }
  }
}

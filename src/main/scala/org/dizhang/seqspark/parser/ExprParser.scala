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
import scala.language.{existentials, higherKinds}
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

  /** literals */
  def lit: Parser[ExprAST] = stringLit | boolLit | numLit

  def stringLit: Parser[ExprAST] = {
    accept("string literal", {
      case StringLit(s) => Str(s)
    })
  }

  def boolLit: Parser[ExprAST] = {
    accept("bool literal", {
      case BooleanLit(b) => Bool(b)
    })
  }

  def numLit: Parser[ExprAST] = {
    accept("number literal", {
      case IntLit(n) => Num(Number(n))
      case DoubleLit(d) => Num(Number(d))
    })
  }

  /** variable */
  def variable: Parser[ExprAST] = id ~ rep(Dot ~ id) ^^ {
    case t ~ ts =>
      val name = ts.foldLeft(t.name){case (acc, _ ~ Identifier(x)) => s"$acc.$x"}
      Variable(name)
  }

  /** function call */
  def func0: Parser[ExprAST] = id ~ LeftParen ~ RightParen ^^ {
    case Identifier(name) ~ _ ~ _ => Func(name, List[ExprAST]())
  }

  def func: Parser[ExprAST] =  id ~ LeftParen ~ expr ~ rep(Comma ~ expr) ~ RightParen ^^ {
    case Identifier(name) ~ _ ~ t ~ ts ~ _ =>
      val args = ts.foldLeft(List(t)){
        case (acc, _ ~ e) => e :: acc
      }.reverse
      Func(name, args)
  }

  /** operators */
  def equalTok: Parser[EQUAL] = {
    accept("equality operator", { case x: EQUAL => x })
  }

  def compTok: Parser[COMP] = {
    accept("comparison operator", { case x: COMP => x })
  }

  def addTok: Parser[ADD] = {
    accept("additive operator", { case x: ADD => x })
  }

  def mulTok: Parser[MUL] = {
    accept("additive operator", { case x: MUL => x })
  }

  /** identifiers */
  def id: Parser[Identifier] = {
    accept("identifier", { case x: Identifier => x })
  }

  /** expression
    * either logical or arithmetic
    * */
  def expr: Parser[ExprAST] = ifelse | or | add

  def simpleExpr: Parser[ExprAST] = lit | variable | LeftParen ~ expr ~ RightParen ^^ {
    case _ ~ e ~ _ => e
  }

  def unaryExpr: Parser[ExprAST] = opt(NOT|addTok) ~ (simpleExpr|func0|func) ^^ {
    case None ~ e => e
    case Some(NOT) ~ e => Not(e)
    case Some(ADD("+")) ~ e => Sign(Operators.Pos, e)
    case Some(ADD("-")) ~ e => Sign(Operators.Neg, e)
  }

  def ifelse: Parser[ExprAST] = IFELSE ~ LeftParen ~ expr ~ Comma ~ expr ~ Comma ~ expr ~ RightParen ^^ {
    case _ ~ _ ~ e1 ~ _ ~ e2 ~ _ ~ e3 ~ _ => IfElse(e1, e2, e3)
  }

  def or: Parser[ExprAST] = and ~ rep(OR ~ and) ^^ {
    case t ~ ts => ts.foldLeft(t){
      case (acc, _ ~ e) => Or(acc, e)
    }
  }

  def and: Parser[ExprAST] = logicFactor ~ rep(AND ~ logicFactor) ^^ {
    case t ~ ts => ts.foldLeft(t){
      case (acc, _ ~ e) => And(acc, e)
    }
  }

  def logicSimple: Parser[ExprAST] = boolLit | variable | equal | comp | LeftParen ~ or ~ RightParen ^^ {
    case _ ~ e ~ _ => e
  }

  def logicFactor: Parser[ExprAST] = opt(NOT) ~ logicSimple ^^ {
    case None ~ e => e
    case Some(NOT) ~ e => Not(e)
  }

  def equal: Parser[ExprAST] = unaryExpr ~ equalTok ~ unaryExpr ^^ {
    case e1 ~ EQUAL("==") ~ e2 => Equal(Operators.EQ, e1, e2)
    case e1 ~ EQUAL("!=") ~ e2 => Equal(Operators.NE, e1, e2)
  }

  def comp: Parser[ExprAST] = add ~ compTok ~ add ^^ {
    case e1 ~ COMP(">=") ~ e2 => Comp(Operators.GE, e1, e2)
    case e1 ~ COMP("<=") ~ e2 => Comp(Operators.LE, e1, e2)
    case e1 ~ COMP(">") ~ e2 => Comp(Operators.GT, e1, e2)
    case e1 ~ COMP("<") ~ e2 => Comp(Operators.LT, e1, e2)
  }

  def add: Parser[ExprAST] = mul ~ rep(addTok ~ mul) ^^ {
    case t ~ ts => ts.foldLeft(t){
      case (acc, ADD("+") ~ e) => Add(Operators.Add, acc, e)
      case (acc, ADD("-") ~ e) => Add(Operators.Sub, acc, e)
    }
  }

  def mul: Parser[ExprAST] = unaryExpr ~ rep(mulTok ~ unaryExpr) ^^ {
    case t ~ ts => ts.foldLeft(t){
      case (acc, MUL("*") ~ e) => Mul(Operators.Mul, acc, e)
      case (acc, MUL("/") ~ e) => Mul(Operators.Div, acc, e)
      case (acc, MUL("%") ~ e) => Mul(Operators.Mod, acc, e)
    }
  }

}

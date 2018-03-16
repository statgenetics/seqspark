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

import spire.math.Number
import scala.util.parsing.input.Positional
import ExprType._

sealed trait ExprAST extends Positional

object ExprAST {

  /** literals */
  case class Bool(value: Boolean) extends ExprAST
  case class Num(value: Number) extends ExprAST
  case class Str(value: String) extends ExprAST


  /** logical expressions */
  case class Or(expr1: ExprAST, expr2: ExprAST) extends ExprAST
  case class And(expr1: ExprAST, expr2: ExprAST) extends ExprAST
  case class Not(expr: ExprAST) extends ExprAST
  case class Comp(op: Operators.CompOp, expr1: ExprAST, expr2: ExprAST) extends ExprAST
  case class Equal(op: Operators.EqualOp, expr1: ExprAST, expr2: ExprAST) extends ExprAST

  /** arithmetic expressions */
  case class Sign(op: Operators.SignOp, expr: ExprAST) extends ExprAST
  case class Add(op: Operators.AddOp, expr1: ExprAST, expr2: ExprAST) extends ExprAST
  case class Mul(op: Operators.MulOp, expr1: ExprAST, expr2: ExprAST) extends ExprAST

  /** conditional */
  case class IfElse[A](condition: ExprAST,
                       branch1: ExprAST,
                       branch2: ExprAST) extends ExprAST

  /** variable */
  case class Variable(name: String) extends ExprAST

  /** function call */
  case class Func(name: String, args: List[ExprAST]) extends ExprAST

  /** compile code and check type */
  def compile(code: String): Either[CompilationError, ExprAST] = {
    val untyped = for {
      tokens <- ExprLexer()(code).right
      ast <- ExprParser()(tokens).right
    } yield ast

    untyped match {
      case Left(_) => untyped
      case Right(ast) => typeInfer(ast) match {
        case Left(te) => Left(te)
        case _ => untyped
      }
    }
  }
}

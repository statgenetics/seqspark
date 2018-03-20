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
import scala.language.higherKinds
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


  def program[repr[_]](exprAST: ExprAST)
                      (implicit kvStore: KVStore[repr],
                       numberAlg: NumberAlg[repr],
                       stringAlg: StringAlg[repr],
                       logicAlg: LogicAlg[repr]): repr[_] = {
    exprAST match {
      case Bool(v) => logicAlg.lit(v)
      case Num(v) => numberAlg.lit(v)
      case Str(v) => stringAlg.lit(v)
      case Or(e1, e2) => logicAlg.or(program(e1), program(e2))
      case And(e1, e2) => logicAlg.and(program(e1), program(e2))
      case Not(e) => logicAlg.not(program(e))
      case Comp(op, e1, e2) => op match {
        case Operators.LT => numberAlg.le(program(e1), program(e2))
        case Operators.LE =>
      }
    }
  }

  def view(exprAST: ExprAST): String = {
    exprAST match {
      case Bool(v) => v.toString
      case Num(v) => v.toString
      case Str(v) => s"'$v'"
      case Or(e1, e2) => s"(${view(e1)} || ${view(e2)})"
      case And(e1, e2) => s"${view(e1)} && ${view(e2)}"
      case Not(e) => s"! ${view(e)}"
      case Comp(op, e1, e2) => s"${view(e1)} $op ${view(e2)}"
      case Equal(op, e1, e2) => s"${view(e1)} $op ${view(e2)}"
      case Sign(op, e) => s"$op ${view(e)}"
      case Add(op, e1, e2) => s"(${view(e1)} $op ${view(e2)})"
      case Mul(op, e1, e2) => s"${view(e1)} $op ${view(e2)}"
      case IfElse(cond, b1, b2) => s"ifelse(${view(cond)}, ${view(b1)}, ${view(b2)})"
      case Variable(n) => n
      case Func(n, lst) => s"$n(${lst.map(e => view(e)).mkString(",")})"
    }
  }

}


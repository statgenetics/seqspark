/*
 * Copyright 2018 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.parser

import scala.language.higherKinds
import ExprAST._
import ExprType._

trait ExprAlg[repr[_]] {
  def get[A](key: String): repr[A]
  def call[A](key: String): repr[A]
  def call[A](key: String, args: repr[_]*): repr[List[A]]
  def error(err: String): repr[String]
  def ifelse[A](cond: repr[Boolean], b1: repr[A], b2: repr[A]): repr[A]
}

object ExprAlg {

    def program[repr[_]](exprAST: ExprAST)
                      (implicit exprAlg: ExprAlg[repr],
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
        case Operators.LT => numberAlg.lt(program(e1), program(e2))
        case Operators.LE => numberAlg.le(program(e1), program(e2))
        case Operators.GT => numberAlg.gt(program(e1), program(e2))
        case Operators.GE => numberAlg.ge(program(e1), program(e2))
      }
      case Equal(op, e1, e2) => (op, typeInfer(e1), typeInfer(e2)) match {
        case (Operators.EQ, Right(NumType), _)|(Operators.EQ, _, Right(NumType)) =>
          numberAlg.eq(program(e1), program(e2))
        case (Operators.NE, Right(NumType), _)|(Operators.NE, _, Right(NumType)) =>
          numberAlg.eq(program(e1), program(e2))
        case (Operators.EQ, Right(BoolType), _)|(Operators.EQ, _, Right(BoolType)) =>
          logicAlg.eq(program(e1), program(e2))
        case (Operators.NE, Right(BoolType), _)|(Operators.NE, _, Right(BoolType)) =>
          logicAlg.ne(program(e1), program(e2))
        case _ =>
          stringAlg(program(e1), program(e2))
      }
      case Sign(op, e) => op match {
        case Operators.Pos => program(e)
        case Operators.Neg => numberAlg.neg(program(e))
      }
      case Add(op, e1, e2) => op match {
        case Operators.Add => numberAlg.add(program(e1), program(e2))
        case Operators.Sub => numberAlg.sub(program(e1), program(e2))
      }
      case Mul(op, e1, e2) =>  op match {
        case Operators.Mul => numberAlg.add(program(e1), program(e2))
        case Operators.Div => numberAlg.sub(program(e1), program(e2))
        case Operators.Mod => numberAlg.sub(program(e1), program(e2))
      }
      case IfElse(cond, b1, b2) => exprAlg.ifelse(program(cond), program(b1), program(b2))
      case Variable(n) => exprAlg.get(n)
      case Func(n, args) => (n.toLowerCase, args.map(e => program(e))) match {
        case ("sum", xs) =>
          if (xs.isEmpty) exprAlg.error("sum function must have argument(s)") else numberAlg.sum(xs: _*)
        case ("mean", xs) =>
          if (xs.isEmpty) exprAlg.error("mean function must have argument(s)") else numberAlg.mean(xs: _*)
        case ("max", xs) =>
          if (xs.isEmpty) exprAlg.error("max function must have argument(s)") else numberAlg.max(xs: _*)
        case ("min", xs) =>
          if (xs.isEmpty) exprAlg.error("min function must have argument(s)") else numberAlg.min(xs: _*)
        case ("log", xs) =>
          if (xs.length != 1)
            exprAlg.error("log function must have one and only one argument")
          else
            numberAlg.log(xs.head)
        case ("pow", xs) =>
          if (xs.length != 1)
            exprAlg.error("pow function must have two and only two arguments")
          else
            numberAlg.pow(xs.head, xs(1))
        case (f, Nil) => exprAlg.call(f)
        case (f, xs) => exprAlg.call(f, xs:_*)
      }
    }
  }

}

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

import cats.free.Free
import scala.util.parsing.input.Positional


sealed trait ExprASTAlg[T] extends Positional

case class BooleanExpr(value: Boolean) extends ExprASTAlg[Boolean]
case class IntExpr(value: Int) extends ExprASTAlg[Int]
case class DoubleExpr(value: Double) extends ExprASTAlg[Double]
case class StringExpr(value: String) extends ExprASTAlg[String]

case class IntVariable(name: String) extends ExprASTAlg[Int]
case class BinaryOp[A, B](op: String, left: ExprASTAlg[B], right: ExprASTAlg[B]) extends ExprASTAlg[A]

case class UnaryOp[A](op: String, operand: ExprASTAlg[A]) extends ExprASTAlg[A]

case class CallFunc[A, B](func: String, args: List[ExprASTAlg[B]]) extends ExprASTAlg[A]

case class IfElse[A](condition: ExprASTAlg[Boolean],
                     branch1: ExprASTAlg[A],
                     branch2: ExprASTAlg[A]) extends ExprASTAlg[A]


object ExprASTAlg {
  type ExprAST[T] = Free[ExprASTAlg, T]

  def booleanExpr(value: Boolean): ExprAST[Boolean] = Free.liftF(BooleanExpr(value))
  def intExpr(value: Int): ExprAST[Int] = Free.liftF(IntExpr(value))
  def doubleExpr(value: Double): ExprAST[Double] = Free.liftF(DoubleExpr(value))
  def stringExpr(value: String): ExprAST[String] = Free.liftF(StringExpr(value))

  def intVariable(name: String): ExprAST[]

}

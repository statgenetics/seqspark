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

import scala.language.{existentials, higherKinds}

object Operators {

  sealed trait ExprOp {
    type Input
    type Output
  }

  sealed trait BinaryOperator extends ExprOp

  sealed trait UnaryOperator extends ExprOp




  /** + operator*/
  case object Add extends BinaryOperator

  /** - operator */
  case object Sub extends BinaryOperator

  /** * operator */
  case object Mul extends BinaryOperator

  /** / operator */
  case object Div extends BinaryOperator

  case object Pos extends UnaryOperator

  case object Neg extends UnaryOperator


  /** && and */
  case object And extends BinaryOperator

  /** || or */
  case object Or extends BinaryOperator

  /** ! not */
  case object Not extends UnaryOperator

  /** < */
  case object LT extends BinaryOperator

  /** <= */
  case object LE extends BinaryOperator

  /** > */
  case object GT extends BinaryOperator

  /** >= */
  case object GE extends BinaryOperator

  /** == */
  case object EQ extends BinaryOperator

  /** != */
  case object NE extends BinaryOperator

  case object Concat extends BinaryOperator

}

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

  sealed trait AddOp

  sealed trait MulOp

  sealed trait SignOp

  sealed trait EqualOp

  sealed trait CompOp

  /** + operator*/
  case object Add extends AddOp

  /** - operator */
  case object Sub extends AddOp

  /** * operator */
  case object Mul extends MulOp

  /** / operator */
  case object Div extends MulOp

  /** % */
  case object Mod extends MulOp

  /** + */
  case object Pos extends SignOp

  /** - */
  case object Neg extends SignOp

  /** < */
  case object LT extends CompOp

  /** <= */
  case object LE extends CompOp

  /** > */
  case object GT extends CompOp

  /** >= */
  case object GE extends CompOp

  /** == */
  case object EQ extends EqualOp

  /** != */
  case object NE extends EqualOp

}

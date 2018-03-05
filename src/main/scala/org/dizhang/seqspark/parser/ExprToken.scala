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

import scala.util.parsing.input.Positional

sealed trait ExprToken extends Positional

object ExprToken {

  /** identifier */
  case class Identifier(name: String) extends ExprToken

  /** string */
  case class StringLit(value: String) extends ExprToken

  /** int */
  case class IntLit(value: Int) extends ExprToken

  /** double */
  case class DoubleLit(value: Double) extends ExprToken

  /** bool */
  case class BooleanLit(value: Boolean) extends ExprToken

  /** if else */
  case object IFELSE extends ExprToken

  /** delimiters */
  case object LeftParen extends ExprToken

  case object RightParen extends ExprToken

  case object Comma extends ExprToken

  case object Dot extends ExprToken

  /** logical operators */
  case object OR extends ExprToken
  case object AND extends ExprToken
  case object NOT extends ExprToken

  /** arithmetic operators */
  case class ADD(name: String) extends ExprToken
  case class MUL(name: String) extends ExprToken

  /** comparison operators */
  case class EQUAL(name: String) extends ExprToken
  case class COMP(name: String) extends ExprToken
}


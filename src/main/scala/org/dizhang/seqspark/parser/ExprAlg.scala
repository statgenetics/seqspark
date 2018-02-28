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

trait ExprAlg[repr[_]] {
  def numAlg: NumberAlg[repr]
  def logicAlg: LogicAlg[repr]
  def stringAlg: StringAlg[repr]

  def intVariable(name: String): repr[Int]
  def doubleVariable(name: String): repr[Double]
  def logicVariable(name: String): repr[Boolean]
  def stringVariable(name: String): repr[String]

  def ifElse[T](cond: repr[Boolean], branch1: repr[T], branch2: repr[T]): repr[T]

}
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

trait Interpreter[T]

object Interpreter {

  case class Eval[T](value: T) extends Interpreter[T]

  def eval[T]: Eval[T] => T = _.value

  case class View[T](info: String) extends Interpreter[T]

  def view[T]: View[T] => String = _.info

  case class Names[T](names: Set[String]) extends Interpreter[T]

  def names[T]: Names[T] => Set[String] = _.names
}

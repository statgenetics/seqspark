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

import org.dizhang.seqspark.parser.Container._

import scala.language.{existentials, higherKinds}

trait StringAlg[repr[_]] {
  def lit(x: String): repr[String]
  def eq(a: repr[String], b: repr[String]): repr[Boolean]
  def ne(a: repr[String], b: repr[String]): repr[Boolean]
  def concat(a: repr[String], b: repr[String]): repr[String]
}

object StringAlg {

  class StringEval extends StringAlg[Eval] {
    def lit(x: String): Eval[String] = Eval(x)
    def eq(a: Eval[String], b: Eval[String]): Eval[Boolean] = Eval(a.value == b.value)
    def ne(a: Eval[String], b: Eval[String]): Eval[Boolean] = Eval(a.value != b.value)
    def concat(a: Eval[String], b: Eval[String]): Eval[String] = Eval(a.value + b.value)
  }

  class StringView extends StringAlg[View] {
    def lit(x: String): View[String] = View(x)
    def eq(a: View[String], b: View[String]): View[Boolean] = View(s"${a.info} == ${b.info}")
    def ne(a: View[String], b: View[String]): View[Boolean] = View(s"${a.info} != ${b.info}")
    def concat(a: View[String], b: View[String]): View[String] = View(s"${a.info} + ${b.info}")
  }

}

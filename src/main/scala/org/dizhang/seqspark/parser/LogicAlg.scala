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
import Interpreter._

trait LogicAlg[repr[_]] {
  def lit(x: Boolean): repr[Boolean]
  def and(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def or(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def not(x: repr[Boolean]): repr[Boolean]
  def eq(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def ne(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
}

object LogicAlg {
  class LogicEval extends LogicAlg[Eval] {
    def lit(x: Boolean): Eval[Boolean] = Eval(x)
    def and(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = Eval(a.value && b.value)
    def or(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = Eval(a.value || b.value)
    def not(x: Eval[Boolean]): Eval[Boolean] = Eval(! x.value)
    def eq(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      Eval(a.value == b.value)
    }
    def ne(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      Eval(a.value != b.value)
    }
  }

  class LogicView extends LogicAlg[View] {
    def lit(x: Boolean): View[Boolean] = View(x.toString)
    def and(a: View[Boolean], b: View[Boolean]): View[Boolean] = View(s"${a.info} and ${b.info}")
    def or(a: View[Boolean], b: View[Boolean]): View[Boolean] = View(s"(${a.info} or ${b.info})")
    def not(x: View[Boolean]): View[Boolean] = View(s"not (${x.info})")
    def eq(a: View[Boolean], b: View[Boolean]): View[Boolean] = {
      View(s"${a.info} == ${b.info}")
    }
    def ne(a: View[Boolean], b: View[Boolean]): View[Boolean] = {
      View(s"${a.info} != ${b.info}")
    }
  }
}
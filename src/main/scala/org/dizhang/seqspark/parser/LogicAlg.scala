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
import Container.View
import cats.Eval

trait LogicAlg[repr[_]] {
  def lit(x: Boolean): repr[Boolean]
  def and(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def or(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def not(x: repr[Boolean]): repr[Boolean]
  def eq(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def ne(a: repr[Boolean], b: repr[Boolean]): repr[Boolean]
  def ifelse[A](cond: repr[Boolean], b1: repr[A], b2: repr[A]): repr[A]
}

object LogicAlg {


  class LogicEval extends LogicAlg[Eval] {
    def lit(x: Boolean): Eval[Boolean] = Eval.now(x)
    def and(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      for {
        x <- a
        y <- b
      } yield x && y
    }
    def or(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      for {
        x <- a
        y <- b
      } yield x || y
    }
    def not(a: Eval[Boolean]): Eval[Boolean] = {
      for (x <- a) yield ! x
    }
    def eq(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      for {
        x <- a
        y <- b
      } yield x == y
    }
    def ne(a: Eval[Boolean], b: Eval[Boolean]): Eval[Boolean] = {
      for {
        x <- a
        y <- b
      } yield x != y
    }

    def ifelse[A](cond: Eval[Boolean], b1: Eval[A], b2: Eval[A]): Eval[A] = {
      for {
        c <- cond
        x <- b1
        y <- b2
      } if (c) x else y
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
    def ifelse[A](cond: View[Boolean], b1: View[A], b2: View[A]): View[A] = {
      View(s"if (${cond.info}) {${b1.info}} else {${b2.info}}")
    }
  }
}
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
 *    WINumberHOUNumber WARRANNumberIES OR CONDINumberIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.seqspark.parser

import org.dizhang.seqspark.parser.Container._
import spire.math.Number
import scala.language.{existentials, higherKinds}

trait NumberAlg[repr[_]] {

  /** repr is a place holder for an interpreter */

  def lit(x: Number): repr[Number]

  def add(a: repr[Number], b: repr[Number]): repr[Number]

  def sub(a: repr[Number], b: repr[Number]): repr[Number]

  def mul(a: repr[Number], b: repr[Number]): repr[Number]

  def div(a: repr[Number], b: repr[Number]): repr[Number]

  def mod(a: repr[Number], b: repr[Number]): repr[Number]

  def pow(a: repr[Number], b: repr[Number]): repr[Number]

  def log(x: repr[Number]): repr[Number]

  def sum(xs: repr[Number]*): repr[Number]

  def max(xs: repr[Number]*): repr[Number]

  def min(xs: repr[Number]*): repr[Number]

  def mean(xs: repr[Number]*): repr[Number]

  def lt(a: repr[Number], b: repr[Number]): repr[Boolean]

  def le(a: repr[Number], b: repr[Number]): repr[Boolean]

  def gt(a: repr[Number], b: repr[Number]): repr[Boolean]

  def ge(a: repr[Number], b: repr[Number]): repr[Boolean]

  def eq(a: repr[Number], b: repr[Number]): repr[Boolean]

  def ne(a: repr[Number], b: repr[Number]): repr[Boolean]

  def neg(x: repr[Number]): repr[Number]

}

object NumberAlg {

  val x: Number = 1
  val y: Number = 1.0
  x/y

  class NumberEval extends NumberAlg[Eval] {

    def lit(x: Number) = Eval(x)
    def add(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value + b.value)
    def sub(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value - b.value)
    def mul(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value * b.value)
    def sum(xs: Eval[Number]*): Eval[Number] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => a + b.value))
    def min(xs: Eval[Number]*): Eval[Number] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => spire.math.min(a, b.value)))
    def max(xs: Eval[Number]*): Eval[Number] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => spire.math.max(a, b.value)))

    def lt(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value < b.value)

    def le(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value <= b.value)

    def gt(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value > b.value)

    def ge(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value >= b.value)

    def eq(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value == b.value)

    def ne(a: Eval[Number], b: Eval[Number]): Eval[Boolean] = Eval(a.value != b.value)

    def neg(x: Eval[Number]): Eval[Number] = Eval(-x.value)

    def div(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value / b.value)

    def mod(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value.toInt % b.value)

    def log(x: Eval[Number]): Eval[Number] = Eval(spire.math.log(x.value))

    def pow(a: Eval[Number], b: Eval[Number]): Eval[Number] = Eval(a.value.pow(b.value))

    def mean(xs: Eval[Number]*): Eval[Number] = Eval(sum(xs: _*).value/Number(xs.length))
  }

  class NumberView extends NumberAlg[View] {

    def lit(x: Number) = View(x.toString)
    def add(a: View[Number], b: View[Number]): View[Number] = View(s"(${a.info} + ${b.info})")
    def sub(a: View[Number], b: View[Number]): View[Number] = View(s"(${a.info} - ${b.info})")
    def mul(a: View[Number], b: View[Number]): View[Number] = View(s"${a.info} * ${b.info}")
    def div(a: View[Number], b: View[Number]): View[Number] = View(s"${a.info} / ${b.info}")
    def mod(a: View[Number], b: View[Number]): View[Number] = View(s"${a.info} % ${b.info}")
    def pow(a: View[Number], b: View[Number]): View[Number] = View(s"pow(${a.info}, ${b.info})")
    def log(x: View[Number]): View[Number] = View(s"log(${x.info})")
    def sum(xs: View[Number]*): View[Number] = View(s"sum(${xs.map(_.info).mkString(",")})")
    def min(xs: View[Number]*): View[Number] = View(s"min(${xs.map(_.info).mkString(",")})")
    def max(xs: View[Number]*): View[Number] = View(s"max(${xs.map(_.info).mkString(",")})")
    def mean(xs: View[Number]*): View[Number] = View(s"mean(${xs.map(_.info).mkString(",")})")
    def neg(x: View[Number]): View[Number] = View(s"-${x.info}")
    def lt(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} < ${b.info})")
    def le(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} <= ${b.info})")
    def gt(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} > ${b.info})")
    def ge(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} >= ${b.info})")
    def eq(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} == ${b.info})")
    def ne(a: View[Number], b: View[Number]): View[Boolean] = View(s"(${a.info} != ${b.info})")
  }

}
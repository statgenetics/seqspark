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

import org.dizhang.seqspark.parser.Interpreter._
import spire.math.Number
import scala.language.{existentials, higherKinds}

trait NumericAlg[num, repr[_]] {

  /** repr is a place holder for an interpreter */

  def lit(x: num): repr[num]

  def add(a: repr[num], b: repr[num]): repr[num]

  def sub(a: repr[num], b: repr[num]): repr[num]

  def mul(a: repr[num], b: repr[num]): repr[num]

  def div(a: repr[num], b: repr[num]): repr[num]

  def mod(a: repr[num], b: repr[num]): repr[num]

  def pow(a: repr[num], b: repr[num]): repr[num]

  def log(x: repr[num]): repr[num]

  def sum(xs: List[repr[num]]): repr[num]

  def max(xs: List[repr[num]]): repr[num]

  def min(xs: List[repr[num]]): repr[num]

  def mean(xs: List[repr[num]]): repr[num]

  def lt(a: repr[num], b: repr[num]): repr[Boolean]

  def le(a: repr[num], b: repr[num]): repr[Boolean]

  def gt(a: repr[num], b: repr[num]): repr[Boolean]

  def ge(a: repr[num], b: repr[num]): repr[Boolean]

  def eq(a: repr[num], b: repr[num]): repr[Boolean]

  def ne(a: repr[num], b: repr[num]): repr[Boolean]

  def neg(x: repr[num]): repr[num]

}

object NumericAlg {

  val x: Number = 1
  val y: Number = 1.0
  x/y
  
  abstract class NumericEval[T: Numeric] extends NumericAlg[T, Eval] {
    private val numeric = implicitly[Numeric[T]]
    import numeric.mkNumericOps

    def lit(x: T) = Eval(x)
    def add(a: Eval[T], b: Eval[T]): Eval[T] = Eval(a.value + b.value)
    def sub(a: Eval[T], b: Eval[T]): Eval[T] = Eval(a.value - b.value)
    def mul(a: Eval[T], b: Eval[T]): Eval[T] = Eval(a.value * b.value)
    def sum(xs: List[Eval[T]]): Eval[T] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => a + b.value))
    def min(xs: List[Eval[T]]): Eval[T] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => numeric.min(a, b.value)))
    def max(xs: List[Eval[T]]): Eval[T] =
      Eval(xs.tail.foldLeft(xs.head.value)((a, b) => numeric.max(a, b.value)))

    def lt(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.lt(a.value, b.value))

    def le(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.lteq(a.value, b.value))

    def gt(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.gt(a.value, b.value))

    def ge(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.gteq(a.value, b.value))

    def eq(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.eq(a.value, b.value))

    def ne(a: Eval[T], b: Eval[T]): Eval[Boolean] = Eval(numeric.ne(a.value, b.value))

    def neg(x: Eval[T]): Eval[T] = Eval(numeric.negate(x.value))
  }

  class IntEval extends NumericEval[Int] {
    def div(a: Eval[Int], b: Eval[Int]): Eval[Int] = Eval(a.value / b. value)
    def mod(a: Eval[Int], b: Eval[Int]): Eval[Int] = Eval(a.value % b. value)
    def pow(a: Eval[Int], b: Eval[Int]): Eval[Int] = Eval(math.pow(a.value, b. value).toInt)
    def log(x: Eval[Int]): Eval[Int] = Eval(math.log(x.value).toInt)
    def mean(xs: List[Eval[Int]]): Eval[Int] = Eval(sum(xs).value/xs.length)
  }

  class DoubleEval extends NumericEval[Double] {
    def div(a: Eval[Double], b: Eval[Double]): Eval[Double] = Eval(a.value / b. value)
    def mod(a: Eval[Double], b: Eval[Double]): Eval[Double] = Eval(a.value % b. value)
    def pow(a: Eval[Double], b: Eval[Double]): Eval[Double] = Eval(math.pow(a.value, b. value).toInt)
    def log(x: Eval[Double]): Eval[Double] = Eval(math.log(x.value).toInt)
    def mean(xs: List[Eval[Double]]): Eval[Double] = Eval(sum(xs).value/xs.length)
  }

  abstract class NumericView[T: Numeric] extends NumericAlg[T, View] {

    def lit(x: T) = View(x.toString)
    def add(a: View[T], b: View[T]): View[T] = View(s"(${a.info} + ${b.info})")
    def sub(a: View[T], b: View[T]): View[T] = View(s"(${a.info} - ${b.info})")
    def mul(a: View[T], b: View[T]): View[T] = View(s"${a.info} * ${b.info}")
    def div(a: View[T], b: View[T]): View[T] = View(s"${a.info} / ${b.info}")
    def mod(a: View[T], b: View[T]): View[T] = View(s"${a.info} % ${b.info}")
    def pow(a: View[T], b: View[T]): View[T] = View(s"pow(${a.info}, ${b.info})")
    def log(a: View[T], b: View[T]): View[T] = View(s"log(${a.info}, ${b.info})")
    def sum(xs: List[View[T]]): View[T] = View(s"sum(${xs.map(_.info).mkString(",")})")
    def min(xs: List[View[T]]): View[T] = View(s"min(${xs.map(_.info).mkString(",")})")
    def max(xs: List[View[T]]): View[T] = View(s"max(${xs.map(_.info).mkString(",")})")
    def mean(xs: List[View[T]]): View[T] = View(s"mean(${xs.map(_.info).mkString(",")})")
    def neg(x: View[T]): View[T] = View(s"-${x.info}")
    def lt(a: View[T], b: View[T]): View[T] = View(s"(${a.info} < ${b.info})")
    def le(a: View[T], b: View[T]): View[T] = View(s"(${a.info} <= ${b.info})")
    def gt(a: View[T], b: View[T]): View[T] = View(s"(${a.info} > ${b.info})")
    def ge(a: View[T], b: View[T]): View[T] = View(s"(${a.info} >= ${b.info})")
    def eq(a: View[T], b: View[T]): View[T] = View(s"(${a.info} == ${b.info})")
    def ne(a: View[T], b: View[T]): View[T] = View(s"(${a.info} != ${b.info})")
  }

}
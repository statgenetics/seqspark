/*
 * Copyright 2017 Zhang Di
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

package org.dizhang.seqspark.numerics

import breeze.linalg._
import breeze.numerics._
import org.slf4j.{Logger, LoggerFactory}
/**
  * Created by zhangdi on 12/13/16.
  */
object Qk21 {
  /**
    * some constants
    * */
  val POINTS = 21

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def concat(input: Array[BigDecimal]): Array[BigDecimal] = {
    val left = input.map(-_)
    val right = input.slice(0, 10).reverse
    left ++ right
  }

  def concat2(input: Array[BigDecimal]): Array[BigDecimal] = {
    val left = input
    val right = input.slice(0, 10).reverse
    left ++ right
  }

  def adjustErr(err: Double, resultAbs: Double, resultAsc: Double): Double = {

    var errAbs = abs(err)
    if (resultAsc != 0.0 && errAbs != 0.0) {
      val scale = pow(200 * errAbs / resultAsc, 1.5)
      //logger.debug(s"scale err: $scale")
      if (scale < 1) {
        errAbs = resultAsc * scale
      } else {
        errAbs = resultAsc
      }
    }
    if (resultAbs > MINVALUE / (50 * EPSILON)) {
      val minErr = 50 * EPSILON * resultAbs
      if (minErr > errAbs) {
        errAbs = minErr
      }
    }
    errAbs
  }



  /** abscissae of the 21-point kronrod rule */
  val xgk0: Array[BigDecimal] =
    """0.995657163025808080735527280689003,
      |0.973906528517171720077964012084452,
      |0.930157491355708226001207180059508,
      |0.865063366688984510732096688423493,
      |0.780817726586416897063717578345042,
      |0.679409568299024406234327365114874,
      |0.562757134668604683339000099272694,
      |0.433395394129247190799265943165784,
      |0.294392862701460198131126603103866,
      |0.148874338981631210884826001129720,
      |0.000000000000000000000000000000000
    |""".stripMargin.split(",").map(x => BigDecimal(x.replaceAll("\\s", "")))

  val xgk = concat(xgk0)

  /** weights of the 10-point gauss rule */
  val wg0: Array[BigDecimal] =
    """0.066671344308688137593568809893332,
      |0.149451349150580593145776339657697,
      |0.219086362515982043995534934228163,
      |0.269266719309996355091226921569469,
      |0.295524224714752870173892994651338
    |""".stripMargin.split(",").map(x => BigDecimal(x.replaceAll("\\s", "")))
  val wg = concat2(wg0)
  /** weights of the 21-point kronrod rule */
  val wgk0: Array[BigDecimal] =
    """0.011694638867371874278064396062192,
      |0.032558162307964727478818972459390,
      |0.054755896574351996031381300244580,
      |0.075039674810919952767043140916190,
      |0.093125454583697605535065465083366,
      |0.109387158802297641899210590325805,
      |0.123491976262065851077958109831074,
      |0.134709217311473325928054001771707,
      |0.142775938577060080797094273138717,
      |0.147739104901338491374841515972068,
      |0.149445554002916905664936468389821
    |""".stripMargin.split(",").map(x => BigDecimal(x.replaceAll("\\s", "")))
  val wgk = concat2(wgk0)

  /** the integrand here is a function of Vector => Vector,
    *  which is more efficient in many cases than
    *  calling a Double => Double function multiple times */
  def apply(f: DenseVector[Double] => DenseVector[Double],
            a: Double, b: Double): (Double, Double, Double, Double) = {
    val center = 0.5 * (a + b)
    val halfLen = 0.5 * (b - a)
    val absHalfLen = abs(halfLen)
    val x: DenseVector[Double] = DenseVector(xgk.map(x => (x * halfLen + center).toDouble))
    val fVals: DenseVector[Double] = f(x)
    val resG = sum(wg.zip(fVals(Range(1, POINTS, 2)).toArray).map(p => (p._1 * p._2).toDouble))
    val resK = sum(wgk.zip(fVals.toArray).map(p => (p._1 * p._2).toDouble))
    val resAbs = sum(wgk.zip(fVals.toArray).map(p => (p._1 * abs(p._2)).toDouble))
    val fMean = 0.5 * resK
    val resAsc = sum(wgk.zip(fVals.toArray).map(p => (p._1 * abs(p._2 - fMean)).toDouble))
    val absErr = Qk21.adjustErr((resK - resG) * halfLen, resAbs * absHalfLen, resAsc * absHalfLen)
    //logger.debug(s"resG: ${resG * halfLen}, resK: ${resK * halfLen}, abserr: $absErr")
    (resK * halfLen, absErr, resAbs * absHalfLen, resAsc * absHalfLen)
  }
}

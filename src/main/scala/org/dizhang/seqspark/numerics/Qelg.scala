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

import breeze.linalg.max
import breeze.numerics.abs
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks._

/**
  * Created by zhangdi on 12/13/16.
  */
object Qelg {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  case class Extrapolation() {
    var n: Int = 0
    var nRes: Int = 0
    val rList2: Array[Double] = Array.fill(52)(0.0)
    val res3La: Array[Double] = Array.fill(3)(0.0)

    def append(y: Double): Unit = {
      rList2(n) = y
      n += 1
    }

    override def toString: String =
      s"""
        |n: $n
        |nRes: $nRes
        |rList2: ${rList2.slice(0, n).map(x => "%.4f".format(x)).mkString(",")}
        |res3La: ${res3La.map(x => "%.4f".format(x)).mkString(",")}
        |""".stripMargin.stripLineEnd
  }
  def apply(table: Extrapolation): (Double, Double) = {
    val epsTab = table.rList2
    val res3La = table.res3La
    val n = table.n - 1

    val current = epsTab(n)

    var absolute = MAXVALUE
    var relative = 5 * EPSILON * abs(current)

    val newElm = n / 2
    val nOrig = n
    var nFinal = n

    val nResOrig = table.nRes

    var result = current
    var absErr = MAXVALUE

    if (n < 2) {
      result = current
      absErr = max(absolute, relative)
      (result, absErr)
    } else {
      epsTab(n + 2) = epsTab(n)
      epsTab(n) = MAXVALUE

      breakable{
        for (i <- 0 until newElm) {
          var res = epsTab(n - 2 * i + 2)
          val e0 = epsTab(n - 2 * i - 2)
          val e1 = epsTab(n - 2 * i - 1)
          val e2 = res

          val e1abs = abs(e1)
          val delta2 = e2 - e1
          val err2 = abs(delta2)
          val tol2 = max(abs(e2), e1abs) * EPSILON
          val delta3 = e1 - e0
          val err3 = abs(delta3)
          val tol3 = max(e1abs, abs(e0)) * EPSILON

          var (e3, delta1, err1, tol1, ss) = (0.0, 0.0, 0.0, 0.0, 0.0)

          if (err2 <= tol2 && err3 <= tol3) {
            result = res
            absolute = err2 + err3
            relative = 5 * EPSILON * abs(res)
            absErr = max(absolute, relative)
            return (result, absErr)
          }

          e3 = epsTab( n - 2 * i)
          epsTab(n - 2 * i) = e1
          delta1 = e1 - e3
          err1 = abs(delta1)
          tol1 = max(e1abs, abs(e3)) * EPSILON

          if (err1 <= tol1 || err2 <= tol2 || err3 <= tol3) {
            nFinal = 2 * i
            break()
          }

          ss = (1/delta1 + 1/delta2) - 1/delta3

          if (abs(ss * e1) <= 0.0001) {
            nFinal = 2 * i
            break()
          }

          res = e1 + 1/ss
          epsTab(n - 2 * i) = res

          {
            val error = err2 + abs(res - e2) + err3
            if (error <= absErr) {
              absErr = error
              result = res
            }
          }
        }
      }

      val limexp = 50 - 1
      if (nFinal == limexp) {
        nFinal = 2 * (limexp/2)
      }

      if (nOrig%2 == 1) {
        for (i<-0 to newElm) {
          epsTab(i * 2 + 1) =  epsTab(i*2 + 3)
        }
      } else {
        for (i<-0 to newElm) {
          epsTab(i * 2) =  epsTab(i*2 + 2)
        }
      }
      if (nOrig != nFinal) {
        for (i <- 0 to nFinal) {
          epsTab(i) = epsTab(nOrig - nFinal + i)
        }
      }
      table.n = nFinal + 1
      if (nResOrig < 3) {
        res3La(nResOrig) = result
        absErr = MAXVALUE
      } else {
        absErr = abs(result - res3La(2)) + abs(result - res3La(1)) + abs(result - res3La(0))
        res3La(0) = res3La(1)
        res3La(1) = res3La(2)
        res3La(2) = result
      }
      table.nRes = nResOrig + 1
      absErr = max(absErr, 5 * EPSILON * abs(result))
      (result, absErr)

    }

  }
}

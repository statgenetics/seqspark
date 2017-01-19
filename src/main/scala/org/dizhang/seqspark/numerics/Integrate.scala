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

/**
  * Created by zhangdi on 12/12/16.
  */
object Integrate {

  def apply(f: DenseVector[Double] => DenseVector[Double],
            a: Double, b: Double,
            epsAbs: Double = 1e-16, epsRel: Double = 1e-6,
            limit: Int = 2000): Qags.Result = {
    val memory = Memory(limit)
    val result = Qags.Result()
    val code: Int = try {
      Qags(f, a, b, epsAbs, epsRel, limit, memory, result)
    } catch {
      case Qags.QagsError(message, _) => println(message); 1
    }
    result
  }

  case class Memory(limit: Int) {
    var size: Int = 0 //active size
    var nrMax: Int = 0
    var maxErr: Int = 0 //maxErr = order(nrMax)
    var maxLevel: Int = 0
    var errMax: Double = 0.0 //errMax = eList(maxErr)
    val aList: Array[Double] = Array.fill[Double](limit)(0.0)
    val bList: Array[Double] = Array.fill[Double](limit)(0.0)
    val rList: Array[Double] = Array.fill[Double](limit)(0.0)
    val eList: Array[Double] = Array.fill[Double](limit)(0.0)
    val order: Array[Int] = Array.fill(limit)(0)
    val level: Array[Int] = Array.fill(limit)(0)

    def init(a: Double, b: Double): Unit = {
      aList(0) = a
      bList(0) = b
    }
    def pushFirst(res: Double, e: Double): Unit = {
      size = 1
      rList(0) = res
      eList(0) = e
    }
    def append(a: Double, b: Double, res: Double, e: Double): Unit = {
      aList(size) = a
      bList(size) = b
      rList(size) = res
      eList(size) = e
      size += 1
    }
    def update(a1: Double, b1: Double, res1: Double, e1: Double,
               a2: Double, b2: Double, res2: Double, e2: Double): Unit = {
      val newLevel = level(maxErr) + 1
      if (e2 > e1) {
        aList(maxErr) = a2
        rList(maxErr) = res2
        eList(maxErr) = e2
        level(maxErr) = newLevel

        aList(size) = a1
        bList(size) = b1
        rList(size) = res1
        eList(size) = e1
        level(size) = newLevel
      } else {
        bList(maxErr) = b1
        rList(maxErr) = res1
        eList(maxErr) = e1
        level(maxErr) = newLevel

        aList(size) = a2
        bList(size) = b2
        rList(size) = res2
        eList(size) = e2
        level(size) = newLevel
      }
      size += 1

      if (newLevel > maxLevel) {
        maxLevel = newLevel
      }

      Qpsrt(this)
    }
    def getMax: (Double, Double, Double, Double) = {
      (aList(maxErr), bList(maxErr), rList(maxErr), eList(maxErr))
    }
    def sumResult: Double = {
      sum(rList.slice(0, size))
    }
    def large_interval: Boolean = {
      level(maxErr) < maxLevel
    }
    def increase_nrmax: Boolean = {
      var jupbnd: Int = 0
      val last = size - 1
      if (last > (1 + limit/2)) {
        jupbnd = limit + 1 - last
      } else {
        jupbnd = last
      }
      for (k <- nrMax to jupbnd) {
        var i_max = order(nrMax)
        maxErr = i_max
        if (level(i_max) < maxLevel) {
          return true
        }
        nrMax += 1
      }
      false
    }
    def reset_nrmax(): Unit = {
      nrMax = 0
      maxErr = order(0)
    }
  }



}

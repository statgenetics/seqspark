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

import breeze.linalg.DenseVector
import breeze.numerics._
import breeze.stats.distributions._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhangdi on 12/14/16.
  */
class IntegrateSpec extends FlatSpec with Matchers {
  def f1(input: DenseVector[Double]): DenseVector[Double] = {
    val dis = new ChiSquared(1.0)
    val dis2 = new ChiSquared(14.0)
    input.map(x => dis.pdf(x) * dis2.cdf(x))
  }
  def sinx(input: DenseVector[Double]): DenseVector[Double] = {
    sin(input)
  }
  def x2(input: DenseVector[Double]): DenseVector[Double] = {
    pow(input, 2.0)
  }

  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
  }

  "A Integrate" should "be well" in {
    time{
      //val res1 = Integrate(f1, 0.0, 40.0)
      //println(s"chisq df=1 pdf|0,1: $res1")
    }("Chisq")

    //val res2 = Integrate(sinx, 0.0, 1.0)
    //val res3 = Integrate(x2, 0.0, 1.0)
    //println(s"sin(x)|0,1: $res2")
    //println(s"x^2|0,1: $res3")
  }
}

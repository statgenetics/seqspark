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
import breeze.numerics.pow
import breeze.stats.distributions.ChiSquared
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhangdi on 12/13/16.
  */
class Qk21Spec extends FlatSpec with Matchers {

  val chisq = ChiSquared(1.0)

  def f(input: DenseVector[Double]): DenseVector[Double] = {
    input.map(x => chisq.pdf(x))
  }

  "A Qk21" should "behave well" in {
    //val res = Qk21(f, 0.0, 1.0)
    //println(res)
  }
}

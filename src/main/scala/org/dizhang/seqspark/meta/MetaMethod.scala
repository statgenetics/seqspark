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

package org.dizhang.seqspark.meta

import breeze.linalg.{*, DenseVector => DV}
import breeze.numerics._
import breeze.stats.distributions._
import org.dizhang.seqspark.util.ConfigValue.WeightMethod
import org.dizhang.seqspark.assoc.{AssocMethod => AME, SKAT => ASKAT, SKATO2 => ASKATO}
import org.dizhang.seqspark.assoc.SumStat.RMWResult
import org.dizhang.seqspark.ds.Variation
import org.dizhang.seqspark.stat.ScoreTest
import org.dizhang.seqspark.util.Constant.Variant

/**
  * Created by zhangdi on 7/11/17.
  */
@SerialVersionUID(7763660001L)
trait MetaMethod {
  def data: RMWResult
  def result: AME.Result
}

object MetaMethod {
  val IK = Variant.InfoKey

  /** currently skat/wss/noWeight are available
    * */
  def getWeight(vars: Array[Variation], weightType: WeightMethod.Value): DV[Double] = {
    lazy val maf = DV(vars.map{v =>
      val mac = v.parseInfo(IK.mac).split(",").map(_.toDouble)
      mac(0)/mac(1)
    })
    weightType match {
      case WeightMethod.skat =>
        val beta = lbeta(1.0, 25.0)
        maf.map(m => 1.0/exp(beta) * pow(1 - m, 24.0))
      case WeightMethod.wss =>
        pow(maf *:* maf.map(1.0 - _), -0.5)
      case _ =>
        DV.ones[Double](vars.length)
    }
  }

  /** Burden test */
  case class Burden(data: RMWResult,
                    weightType: WeightMethod.Value) extends MetaMethod {
    def result: AME.Result = {
      val weight = getWeight(data.vars, weightType)
      val u = weight dot data.score
      val v =  weight dot (data.variance * weight)
      val stat = u/math.sqrt(v)
      val dis = Gaussian(0.0, 1.0)
      val pValue = 1.0 - dis.cdf(stat)
      AME.BurdenAnalytic(data.vars, stat, Some(stat), s"test=score")
    }
  }

  /** SKAT */
  case class SKAT(data: RMWResult,
                  weightType: WeightMethod.Value,
                  numericalMethod: String,
                  rho: Double) extends MetaMethod {
    def result: AME.Result = {
      val weight = getWeight(data.vars, weightType)
      val u = weight *:* data.score
      val tmp =  data.variance(::, *) *:* weight
      val v = tmp(*, ::) *:* weight
      val st = ScoreTest.Mock(u, v)
      ASKAT(st, data.vars, numericalMethod, rho).result
    }
  }

  /** SKAT-O */

  case class SKATO(data: RMWResult,
                   weightType: WeightMethod.Value,
                   numericalMethod: String,
                   rhos: Array[Double]) extends MetaMethod {
    def result: AME.Result = {
      val weight = getWeight(data.vars, weightType)
      val u = weight *:* data.score
      val tmp =  data.variance(::, *) *:* weight
      val v = tmp(*, ::) *:* weight
      val st = ScoreTest.Mock(u, v)
      ASKATO(st, data.vars, numericalMethod, rhos).result
    }
  }

}
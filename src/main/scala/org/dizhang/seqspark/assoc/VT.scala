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

package org.dizhang.seqspark.assoc

import breeze.linalg._
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.stat.{Resampling, ScoreTest}
import org.dizhang.seqspark.util.General.RichDouble
import org.slf4j.LoggerFactory

import scala.language.existentials

/**
  * Variable threshold association method
  */
@SerialVersionUID(7727880001L)
trait VT extends AssocMethod {
  def nullModel: NM
  def x: Encode.VT
  def result: AssocMethod.Result
}

object VT {

  val logger = LoggerFactory.getLogger(getClass)

  def apply(nullModel: NM,
            x: Encode.Coding): VT with AssocMethod.AnalyticTest = {
    val nmf = nullModel match {
      case NM.Simple(y, b) => NM.Fit(y, b)
      case NM.Mutiple(y, c, b) => NM.Fit(y, c, b)
      case nm: NM.Fitted => nm
    }
    AnalyticScoreTest(nmf, x.asInstanceOf[Encode.VT])
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NM.Fitted,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.VT])
  }

  def getStatistic(nm: NM.Fitted, x: Encode.Coding): Double = {
    //println(s"scores: ${st.score.toArray.mkString(",")}")
    //println(s"variances: ${diag(st.variance).toArray.mkString(",")}")
    val m = x.asInstanceOf[Encode.VT].coding
    val ts = m.map{sv =>
      val st = ScoreTest(nm, sv)
      st.score(0)/st.variance(0, 0).sqrt
    }
    //val ts = st.score :/ diag(st.variance).map(x => x.sqrt)
    max(ts)
  }

  @SerialVersionUID(7727880101L)
  final case class AnalyticScoreTest(nullModel: NM.Fitted,
                                     x: Encode.VT)
    extends VT with AssocMethod.AnalyticTest
  {

    val statistic = getStatistic(nullModel, x)
    val pValue = None
    def result: AssocMethod.VTAnalytic = {
      val info = s"MAFs=${x.coding.length}"
      AssocMethod.VTAnalytic(x.vars, x.size, statistic, pValue, info)
    }
  }

  @SerialVersionUID(7727880201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NM.Fitted,
                                  x: Encode.VT)
    extends VT with AssocMethod.ResamplingTest
  {
    def pCount = {
      Resampling.Simple(refStatistic, min, max, nullModel, x, getStatistic).pCount
    }
    def result: AssocMethod.VTResampling =
      AssocMethod.VTResampling(x.vars, x.size, refStatistic, pCount)
  }

}

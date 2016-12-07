package org.dizhang.seqspark.assoc

import breeze.numerics.abs
import breeze.stats.distributions.Gaussian
import org.dizhang.seqspark.stat.ScoreTest.NullModel
import org.dizhang.seqspark.stat.{Resampling, ScoreTest}
import org.dizhang.seqspark.util.General._

import scala.language.existentials

/**
  * Created by zhangdi on 12/1/16.
  */
trait SNV extends AssocMethod {
  def nullModel: NullModel
  def x: Encode.Common
  def result: AssocMethod.Result
}

object SNV {
  def apply(nullModel: NullModel,
            x: Encode.Coding): AnalyticTest = {
    AnalyticTest(nullModel, x.asInstanceOf[Encode.Common])
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NullModel,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.Common])
  }

  def getStatistic(st: ScoreTest): Double = {
    abs(st.score(0))/st.variance(0,0).sqrt
  }

  @SerialVersionUID(7727280101L)
  final case class AnalyticTest(nullModel: NullModel,
                                x: Encode.Common) extends SNV with AssocMethod.AnalyticTest {
    val scoreTest = ScoreTest(nullModel, x.coding)
    val statistic = getStatistic(scoreTest)
    val pValue = {
      val dis = new Gaussian(0.0, 1.0)
      Some((1.0 - dis.cdf(statistic)) * 2)
    }

    def result: AssocMethod.AnalyticResult = {
      AssocMethod.AnalyticResult(x.vars, statistic, pValue)
    }
  }

  @SerialVersionUID(7727280201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NullModel,
                                  x: Encode.Common) extends SNV with AssocMethod.ResamplingTest {
    def pCount = Resampling.Simple(refStatistic, min, max, nullModel, x, getStatistic).pCount
    def result: AssocMethod.ResamplingResult = {
      AssocMethod.ResamplingResult(x.vars, refStatistic, pCount)
    }
  }
}

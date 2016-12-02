package org.dizhang.seqspark.assoc

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
  def x: Encode[_]
  def result: AssocMethod.Result
}

object SNV {
  def apply(nullModel: NullModel,
            x: Encode[_]): Burden = {
    AnalyticTest(nullModel, x)
  }

  def getStatistic(st: ScoreTest): Double = {
    st.score(0)/st.variance(0,0).sqrt
  }

  @SerialVersionUID(7727280101L)
  final case class AnalyticTest(nullModel: NullModel,
                                x: Encode[_]) extends Burden with AssocMethod.AnalyticTest {
    val geno = x.getCommon().get
    val scoreTest = ScoreTest(nullModel, geno.coding)
    val statistic = getStatistic(scoreTest)
    val pValue = {
      val dis = new Gaussian(0.0, 1.0)
      Some(1.0 - dis.cdf(statistic))
    }

    def result: AssocMethod.AnalyticResult = {
      AssocMethod.AnalyticResult(geno.vars, statistic, pValue)
    }
  }

  @SerialVersionUID(7727280201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NullModel,
                                  x: Encode[_]) extends Burden with AssocMethod.ResamplingTest {
    val geno = x.getCommon().get
    def pCount = Resampling.Test(refStatistic, min, max, nullModel, x, getStatistic).pCount
    def result: AssocMethod.ResamplingResult = {
      AssocMethod.ResamplingResult(geno.vars, refStatistic, pCount)
    }
  }
}

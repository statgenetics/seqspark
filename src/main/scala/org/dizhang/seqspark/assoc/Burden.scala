package org.dizhang.seqspark.assoc

import breeze.stats.distributions.Gaussian
import org.dizhang.seqspark.stat.ScoreTest.NullModel
import org.dizhang.seqspark.stat.{Resampling, ScoreTest}
import org.dizhang.seqspark.util.General._

import scala.language.existentials

/**
  * Created by zhangdi on 5/20/16.
  */
@SerialVersionUID(7727280001L)
trait Burden extends AssocMethod {
  def nullModel: NullModel
  def x: Encode.Fixed
  def result: AssocMethod.Result
}

object Burden {

  def apply(nullModel: NullModel,
            x: Encode.Coding): AnalyticTest = {
    AnalyticTest(nullModel, x.asInstanceOf[Encode.Fixed])
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NullModel,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.Fixed])
  }

  def getStatistic(st: ScoreTest): Double = {
    st.score(0)/st.variance(0,0).sqrt
  }

  @SerialVersionUID(7727280101L)
  final case class AnalyticTest(nullModel: NullModel,
                                x: Encode.Fixed) extends Burden with AssocMethod.AnalyticTest {
    def geno = x.coding
    val scoreTest = ScoreTest(nullModel, geno)
    val statistic = getStatistic(scoreTest)
    val pValue = {
      val dis = new Gaussian(0.0, 1.0)
      Some(1.0 - dis.cdf(statistic))
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
                                  x: Encode.Fixed) extends Burden with AssocMethod.ResamplingTest {
    def pCount = Resampling.Simple(refStatistic, min, max, nullModel, x, getStatistic).pCount
    def result: AssocMethod.ResamplingResult = {
      AssocMethod.ResamplingResult(x.vars, refStatistic, pCount)
    }
  }
}
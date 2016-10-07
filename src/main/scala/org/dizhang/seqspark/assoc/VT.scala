package org.dizhang.seqspark.assoc

import breeze.linalg._
import org.dizhang.seqspark.stat.{MultivariateNormal, Resampling, ScoreTest}
import org.dizhang.seqspark.stat.ScoreTest.NullModel
import org.dizhang.seqspark.util.General.{RichDouble}
import scala.language.existentials

/**
  * Variable threshold association method
  */
@SerialVersionUID(7727880001L)
trait VT extends AssocMethod {
  def nullModel: NullModel
  def x: Encode[_]
  def result: AssocMethod.Result
}

object VT {

  def getStatistic(st: ScoreTest): Double = {
    val ts = st.score :/ diag(st.variance).map(x => x.sqrt)
    max(ts)
  }

  @SerialVersionUID(7727880101L)
  final case class AnalyticTest(nullModel: NullModel,
                                x: Encode[_]) extends VT with AssocMethod.AnalyticTest {
    val geno = x.getVT.get
    val scoreTest = ScoreTest(nullModel, geno.coding)
    val statistic = getStatistic(scoreTest)
    val pValue = {
      val ss = diag(scoreTest.variance).map(_.sqrt)
      val ts = scoreTest.score :/ ss
      val maxT = max(ts)
      val cutoff = maxT * ss
      val dis = MultivariateNormal.Centered(scoreTest.variance)
      dis.cdf(cutoff).pvalue
    }
    def result = AssocMethod.AnalyticResult(geno.vars, statistic, pValue)
  }

  @SerialVersionUID(7727880201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NullModel,
                                  x: Encode[_]) extends VT with AssocMethod.ResamplingTest {
    def pCount = {
      Resampling.Test(refStatistic, min, max, nullModel, x, getStatistic).pCount
    }
    def result = AssocMethod.ResamplingResult(x.getVT.get.vars, refStatistic, pCount)
  }

}

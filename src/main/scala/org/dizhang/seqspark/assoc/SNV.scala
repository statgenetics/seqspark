package org.dizhang.seqspark.assoc

import breeze.stats.distributions.Gaussian
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.stat.{Resampling, ScoreTest, WaldTest}
import org.dizhang.seqspark.util.General._

import scala.language.existentials

/**
  * Created by zhangdi on 12/1/16.
  */
trait SNV extends AssocMethod {
  def nullModel: NM
  def x: Encode.Common
  def result: AssocMethod.Result
}

object SNV {
  def apply(nullModel: NM,
            x: Encode.Coding): SNV with AssocMethod.AnalyticTest = {
    nullModel match {
      case nm: NM.Fitted =>
        AnalyticScoreTest(nm, x.asInstanceOf[Encode.Common])
      case _ =>
        AnalyticWaldTest(nullModel, x.asInstanceOf[Encode.Common])
    }
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NM.Fitted,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.Common])
  }

  def getStatistic(nm: NM.Fitted, x: Encode.Coding): Double = {
    val st = ScoreTest(nm, x.asInstanceOf[Encode.Common].coding)
    math.abs(st.score(0)/st.variance(0,0).sqrt)
  }

  @SerialVersionUID(7727280101L)
  final case class AnalyticScoreTest(nullModel: NM.Fitted,
                                     x: Encode.Common)
    extends SNV with AssocMethod.AnalyticTest
  {
    //val scoreTest = ScoreTest(nullModel, x.coding)
    val statistic = getStatistic(nullModel, x)
    val pValue = {
      val dis = new Gaussian(0.0, 1.0)
      Some((1.0 - dis.cdf(statistic)) * 2)
    }

    def result: AssocMethod.BurdenAnalytic = {
      AssocMethod.BurdenAnalytic(x.vars, statistic, pValue, "test=score")
    }
  }

  case class AnalyticWaldTest(nullModel: NM,
                              x: Encode.Common)
    extends SNV with AssocMethod.AnalyticTest
  {
    private val wt = WaldTest(nullModel, x.coding.toDenseVector)
    val statistic = wt.beta(1) / wt.std(1)
    val pVaue = Some(wt.pValue(oneSided = false).apply(1))
    def result = {
      AssocMethod.BurdenAnalytic(x.vars, statistic, pVaue, s"test=wald;beta=${wt.beta(1)};betaStd=${wt.std(1)}")
    }
  }

  @SerialVersionUID(7727280201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NM.Fitted,
                                  x: Encode.Common)
    extends SNV with AssocMethod.ResamplingTest
  {
    def pCount = Resampling.Simple(refStatistic, min, max, nullModel, x, getStatistic).pCount
    def result: AssocMethod.BurdenResampling = {
      AssocMethod.BurdenResampling(x.vars, refStatistic, pCount)
    }
  }
}

package org.dizhang.seqspark.assoc

import breeze.linalg.DenseVector
import breeze.stats.distributions.{Gaussian, StudentsT}
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.stat.{Resampling, ScoreTest, WaldTest}
import org.dizhang.seqspark.util.General._

import scala.language.existentials

/**
  * Created by zhangdi on 5/20/16.
  */
@SerialVersionUID(7727280001L)
trait Burden extends AssocMethod {
  def nullModel: NM
  def x: Encode.Fixed
  def result: AssocMethod.Result
}

object Burden {

  def apply(nullModel: NM,
            x: Encode.Coding): Burden with AssocMethod.AnalyticTest = {
    nullModel match {
      case nm: NM.Fitted =>
        AnalyticScoreTest(nm, x.asInstanceOf[Encode.Fixed])
      case _ =>
        AnalyticWaldTest(nullModel, x.asInstanceOf[Encode.Fixed])
    }
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NM.Fitted,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.Fixed])
  }

  def getStatistic(nm: NM.Fitted, x: Encode.Coding): Double = {
    val st = ScoreTest(nm, x.asInstanceOf[Encode.Fixed].coding)
    st.score(0)/st.variance(0,0).sqrt
  }

  def getStatistic(nm: NM, x: DenseVector[Double]): Double = {
    val wt = WaldTest(nm, x)
    (wt.beta :/ wt.std).apply(1)
  }

  @SerialVersionUID(7727280101L)
  final case class AnalyticScoreTest(nullModel: NM.Fitted,
                                     x: Encode.Fixed)
    extends Burden with AssocMethod.AnalyticTest
  {
    def geno = x.coding
    //val scoreTest = ScoreTest(nullModel, geno)
    val statistic = getStatistic(nullModel, x)
    val pValue = {
      val dis = new Gaussian(0.0, 1.0)
      Some(1.0 - dis.cdf(statistic))
    }

    def result: AssocMethod.BurdenAnalytic = {
      AssocMethod.BurdenAnalytic(x.vars, statistic, pValue, "test=score")
    }

  }
  case class AnalyticWaldTest(nullModel: NM,
                              x: Encode.Fixed) extends Burden with AssocMethod.AnalyticTest {
    def geno = x.coding
    private val wt = WaldTest(nullModel, x.coding)
    val statistic = getStatistic(nullModel, geno)
    val pValue = {
      val dis = new StudentsT(nullModel.dof - 1)
      Some(1.0 - dis.cdf(statistic))
    }
    def result = {
      AssocMethod.BurdenAnalytic(x.vars, statistic, pValue, s"test=wald;beta=${wt.beta(1)};betaStd=${wt.std(1)}")
    }
  }

  @SerialVersionUID(7727280201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NM.Fitted,
                                  x: Encode.Fixed)
    extends Burden with AssocMethod.ResamplingTest
  {
    def pCount = Resampling.Simple(refStatistic, min, max, nullModel, x, getStatistic).pCount
    def result: AssocMethod.BurdenResampling = {
      AssocMethod.BurdenResampling(x.vars, refStatistic, pCount)
    }
  }
}
package org.dizhang.seqspark.assoc

import breeze.linalg._
import org.dizhang.seqspark.stat.ScoreTest.NullModel
import org.dizhang.seqspark.stat.{MultivariateNormal, Resampling, ScoreTest}
import org.dizhang.seqspark.util.General.RichDouble

import scala.language.existentials
import scala.util.{Success, Try}

/**
  * Variable threshold association method
  */
@SerialVersionUID(7727880001L)
trait VT extends AssocMethod {
  def nullModel: NullModel
  def x: Encode.VT
  def result: AssocMethod.Result
}

object VT {

  def apply(nullModel: NullModel,
            x: Encode.Coding): AnalyticTest = {
    AnalyticTest(nullModel, x.asInstanceOf[Encode.VT])
  }

  def apply(ref: Double, min: Int, max: Int,
            nullModel: NullModel,
            x: Encode.Coding): ResamplingTest = {
    ResamplingTest(ref, min, max, nullModel, x.asInstanceOf[Encode.VT])
  }

  def getStatistic(nm: NullModel, x: Encode.Coding): Double = {
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
  final case class AnalyticTest(nullModel: NullModel,
                                x: Encode.VT)
    extends VT with AssocMethod.AnalyticTest
  {
    val scoreTest = {
      //val cnt = x.coding.activeSize
      //println(s"geno: ${geno.coding.rows} x ${geno.coding.cols}, " +
      //  s"active: $cnt sample: ${geno.coding.toDense(0,::).t.toArray.mkString(",")}")
      val m = DenseVector.horzcat(x.coding:_*)
      ScoreTest(nullModel, m)
    }
    val statistic = getStatistic(nullModel, x)
    val pValue = {
      val ss = diag(scoreTest.variance).map(_.sqrt)
      val ts = scoreTest.score :/ ss
      val maxT = max(ts)
      val cutoff = maxT * ss
      val pTry = Try(MultivariateNormal.Centered(scoreTest.variance).cdf(cutoff).pvalue)
      pTry match {
        case Success(p) => Some(1.0 - p)
        case _ => None
      }
    }
    def result: AssocMethod.VTAnalytic =
      AssocMethod.VTAnalytic(x.vars, x.size, statistic, pValue)
  }

  @SerialVersionUID(7727880201L)
  final case class ResamplingTest(refStatistic: Double,
                                  min: Int,
                                  max: Int,
                                  nullModel: NullModel,
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

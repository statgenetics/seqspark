package org.dizhang.seqspark.assoc

import breeze.linalg._
import breeze.numerics.{abs, pow}
import breeze.stats._
import breeze.linalg.{CSCMatrix => CM, DenseMatrix => DM, DenseVector => DV}
import SKATO._
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.General._

import collection.JavaConverters._
import breeze.integrate._
import org.dizhang.seqspark.stat.ScoreTest.{LinearModel => STLinear, LogisticModel => STLogistic, NullModel => STNull}
import scala.language.existentials
/**
  * optimal SKAT test
  *
  * follow the R package implementation
  *
  */

object SKATO {
  val RhosOld = (0 to 10).map(x => x * 1.0/10.0).toArray
  val RhosAdj = Array(0.0, 0.01, 0.04, 0.09, 0.16, 0.25, 0.5, 1.0)

  def apply(nullModel: NullModel,
            x: Encode[_]): SKATO = {
    val method = x.config.misc.getString("method")
    method match {
      case "davies" => Davies(nullModel, x)
      case _ => LiuModified(nullModel, x)
    }
  }

  def getParameters(p0sqrtZ: DM[Double],
                    rs: Array[Double],
                    pi: Option[DV[Double]] = None,
                    resampled: Option[DM[Double]] = None) : Parameters = {
    val numSamples = p0sqrtZ.rows
    val numVars = p0sqrtZ.cols
    val meanZ: DV[Double] = sum(p0sqrtZ(*, ::))/numVars.toDouble
    val meanZmat: DM[Double] = DM.zeros[Double](numSamples, numVars)
    meanZmat(::, *) := meanZ
    val cof1 = (meanZ.t * meanZmat).t/sum(pow(meanZ, 2))
    val iterm1 = meanZmat * diag(cof1)
    val iterm2 = p0sqrtZ - iterm1
    /** w3 is the mixture chisq term
      * */
    val w3 = iterm2.t * iterm2

    val varZeta = sum(iterm1.t * iterm1 * w3) * 4

    val (lambda, u) = SKAT.getLambdaU(w3)
    val muQ = sum(lambda)
    val (varQ, kurQ) = resampled match {
      case None =>
        val v = sum(pow(lambda, 2)) + varZeta
        val k = sum(pow(lambda, 4))/sum(pow(lambda, 2)).square * 12
        (v, k)
      case Some(res) =>
        val qTmp = pow(res * iterm2, 2)
        val qs = sum(qTmp(*, ::))
        val dis = new LCCSResampling(lambda, u, pi.get, qs)
        (dis.varQ + varZeta, dis.kurQ)
    }
    val sumCof2 = sum(pow(cof1, 2))
    val meanZ2 = sum(pow(meanZ, 2))
    lazy val taus = rs.map{r =>
      meanZ2 * (numVars.toDouble.square * r + (1 - r) * sumCof2)}
    Parameters(muQ, varQ, kurQ, lambda, varZeta, taus)
  }

  /**
    * Why Not use the Kurtosis in the LCCSResampling???
    *
    * Compute the Kurtosis for the generalized SKAT statistic Q(rho)
    * (1 - rho) * kappa + tau(rho) * eta
    *
    * kappa ~ LCCS(lambda, df = Ones) + Zeta
    * eta ~ ChiSquared(df = 1)
    * */
  def getKurtosis(df1: Double, // df of kappa
                  df2: Double, // df of eta, usually 1.0
                  v1: Double, // varQ = Var(Qs) + Var(Zeta)
                  a1: Double, // 1 - rho(i)
                  a2: Double // tau(i)
                         ): Double = {
    val v2 = 2 * df2
    val s41 = (12/df1 + 3) * v1.square
    val s42 = (12/df2 + 3) * v2.square
    val s4 = pow(a1, 4) * s41 + pow(a2, 4) * s42 + 6 * a1.square * a2.square * v1 * v2
    val s2 = a1.square * v1 + a2.square * v2
    val k = s4/s2.square - 3
    if (k < 0) 0.0001 else k
  }

  @SerialVersionUID(302L)
  trait NullModel extends Regression.Result with Serializable {
    val informationInverse = inv(information)
    def STNullModel: STNull
  }

  object NullModel {
    def apply(reg: Regression): NullModel = {
      reg match {
        case _: LogisticRegression =>
          LogisticModel(reg.responses, reg.estimates, reg.xs, reg.information)
        case _ =>
          LinearModel(reg.responses, reg.estimates, reg.xs, reg.information)
      }
    }
  }

  case class LinearModel(responses: DV[Double],
                         estimates: DV[Double],
                         xs: DM[Double],
                         information: DM[Double]) extends NullModel {
    val STNullModel: STLinear = STLinear(responses, estimates, xs, information)
    val sigma = STNullModel.residualsVariance.sqrt
    val xsInfoInv = xs * informationInverse * STNullModel.residualsVariance
  }
  case class LogisticModel(responses: DV[Double],
                           estimates: DV[Double],
                           xs: DM[Double],
                           information: DM[Double]) extends NullModel {
    val STNullModel: STLogistic = STLogistic(responses, estimates, xs, information)
    def variance = STNullModel.residualsVariance
    val sigma = variance.map(p => p.sqrt)
    val xsInfoInv = (xs(::, *) :* sigma) * informationInverse
  }

  @SerialVersionUID(303L)
  case class Parameters(muQ: Double,
                        varQ: Double,
                        kurQ: Double,
                        lambda: DV[Double],
                        varZeta: Double,
                        taus: Array[Double]) extends Serializable {
    def df = 12.0/kurQ
  }

  final case class Moments(muQ: Double, varQ: Double, df: Double)


  trait AsymptoticKur extends SKATO {

    val param = getParameters(P0SqrtZ, rhos)

    val pValues = {
      val cdf = lambdas.zip(qScores).map{case (l, q) =>
        (l, q, LCCSDavies.Simple(l).cdf(q))}
      for ((l, q, c) <- cdf) yield
        if (c.pvalue < 0.0 || c.pvalue >1.0) {
          1.0 - LCCSLiu.Modified(l).cdf(q).pvalue
        } else {
          1.0 - c.pvalue
        }
    }
    val pMinQuantiles = {
      lambdas.map{lb =>
        val lm = LCCSLiu.Modified(lb)
        val df = lm.df
        val chiDis = new ChiSquared(df)
        val qOrig = chiDis.inverseCdf(1.0 - pMin)
        (qOrig - df)/(2 * df).sqrt * lm.sigmaQ + lm.muQ
      }
    }
  }

  @SerialVersionUID(7727760101L)
  case class Davies(nullModel: NullModel,
                    x: Encode[_]) extends SKATO with AsymptoticKur {
    lazy val term2 = new ChiSquared(1.0)

    def integralFunc(x: Double): Double = {
      val tmp1 = DV(param.taus: _*) * x
      val tmp = (DV(pMinQuantiles: _*) - tmp1) :/ DV(rhos.map(1.0 - _): _*)
      val kappa = min(tmp)
      val term1 =
        if (kappa > sum(param.lambda) * 1e4) {
          1.0
        } else {
          val tmpQ = (kappa - param.muQ) * (param.varQ - param.varZeta).sqrt / param.varQ.sqrt + param.muQ
          LCCSDavies.Simple(param.lambda).cdf(tmpQ).pvalue
        }
      term1 * term2.pdf(x)
    }
    def pValue: Double = {
      val re = simpson(integralFunc, 0.0, 40.0, 2000)
      1.0 - re
    }
  }

  @SerialVersionUID(7727760201L)
  trait LiuPValue extends SKATO {

    lazy val term1 = new ChiSquared(df)
    lazy val term2 = new ChiSquared(1.0)

    def integralFunc(x: Double): Double = {
      val tmp1 = DV(param.taus: _*) * x
      val tmp = (DV(pMinQuantiles: _*) - tmp1) :/ DV(rhos.map(1.0 - _): _*)
      val tmpMin = min(tmp)
      val tmpQ = (tmpMin - param.muQ)/param.varQ.sqrt * (2 * df).sqrt + df
      term1.cdf(tmpQ) * term2.pdf(x)
    }
    def pValue: Double = {
      val re = simpson(integralFunc, 0.0, 40.0, 2000)
      1.0 - re
    }
  }

  @SerialVersionUID(7727760301L)
  case class LiuModified(nullModel: NullModel,
                         x: Encode[_]) extends LiuPValue with AsymptoticKur {
    lazy val kurQ = {
      12.0 * sum(pow(param.lambda, 4))/sum(pow(param.lambda, 2)).square
    }
  }

  case class SmallSampleAdjust(nullModel: LogisticModel,
                               x: Encode[_],
                               resampled: DM[Double]) extends LiuPValue {

    val param = getParameters(P0SqrtZ, rhos, Some(nullModel.variance), Some(resampled))

    val simScores = resampled * geno


    val pValues = {
      rhos.indices.map{i =>
        val simQs = simScores(*, ::).map(s => s.t * kernels(i) * s)
        1.0 - new LCCSResampling(lambdas(i), us(i), nullModel.variance, simQs).cdf(qScores(i)).pvalue
      }.toArray
    }

    val pMinQuantiles = {
      rhos.indices.map{i =>
        val varRho = (1 - rhos(i)).sqrt * param.varQ + param.taus(i).sqrt * 2
        val kurRho = getKurtosis(param.df, 1.0, param.varQ, 1 - rhos(i), param.taus(i))
        val dfRho = 12.0/kurRho
        val qOrig = new ChiSquared(dfRho).inverseCdf(1 - pMin)
        (qOrig - dfRho)/(2 * dfRho).sqrt * varRho.sqrt + sum(lambdas(i))
      }.toArray
    }
  }
}

@SerialVersionUID(7727760001L)
trait SKATO extends AssocMethod with AssocMethod.AnalyticTest {
  def nullModel: NullModel
  def x: Encode[_]
  lazy val geno: CM[Double] = x.getRare().get.coding
  lazy val weight = x.weight()
  def numVars = weight.length
  lazy val misc = x.config.misc
  lazy val method = misc.getString("method")
  lazy val rhos: Array[Double] = {
    method match {
      case "optimal.adj" => RhosAdj
      case "optimal" => RhosOld
      case _ => misc.getDoubleList("rCorr").asScala.toArray.map(_.toDouble)
    }
  }
  /**
    * we don't store P0 or P0sqrt, because n x n matrix could be too large
    * instead, we always directly compute and store P0sqrt * Z
    *
    *  */
  lazy val P0SqrtZ: DM[Double] = {
    val z: CM[Double] = rowMultiply(geno, weight)
    nullModel match {
      case lm: LinearModel =>
        (- lm.xsInfoInv * (lm.xs.t * z) + z)/lm.sigma
      case lm: LogisticModel =>
        colMultiply(z, lm.sigma).toDense - lm.xsInfoInv * (lm.xs.t * colMultiply(z, lm.variance))
    }
  }

  def param: Parameters

  def df = param.df

  lazy val scoreTest = ScoreTest(nullModel.STNullModel, geno)

  lazy val score = scoreTest.score

  lazy val scoreSigma = symMatrixSqrt(scoreTest.variance)

  lazy val kernels: Array[DM[Double]] = {
    val i = DM.eye[Double](numVars)
    val o = DM.ones[Double](numVars, numVars)
    rhos.map(r => diag(weight) * ((1 - r) * i + r * o) * diag(weight))
  }

  lazy val qScores: Array[Double] = kernels.map(k => score.t * k * score)

  lazy val vcs = kernels.map(k => scoreSigma * k * scoreSigma)

  lazy val (lambdas, us): (Array[DV[Double]], Array[DM[Double]]) = {
    val res = vcs.map(v => SKAT.getLambdaU(v))
    (res.map(_._1), res.map(_._2))
  }
  def pValues: Array[Double]

  def pMin = min(pValues)

  def pMinQuantiles: Array[Double]

  def pValue: Double

  def result = AssocMethod.AnalyticResult(x.getRare().get.vars, 1.0 - pValue, pValue)

}

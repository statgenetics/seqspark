package org.dizhang.seqspark.assoc

import breeze.numerics._
import breeze.linalg.{CSCMatrix => CM, DenseMatrix => DM, DenseVector => DV, _}
import breeze.numerics.{lgamma, pow}
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.assoc.SKATO._
import org.dizhang.seqspark.stat.ScoreTest.{LinearModel => STLinear, LogisticModel => STLogistic, NullModel => STNull}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.General._

import scala.language.existentials
/**
  * optimal SKAT test
  *
  * follow the R package implementation
  *
  */

object SKATO {
  val RhosOld = (0 to 9).map(x => x * 1.0/10.0).toArray :+ 0.999
  val RhosAdj = Array(0.0, 0.01, 0.04, 0.09, 0.16, 0.25, 0.5, 0.999)

  def apply(nullModel: NullModel,
            x: Encode.Coding,
            method: String): SKATO = {
    method match {
      case "davies" => Davies(nullModel, x.asInstanceOf[Encode.Rare], method)
      case _ => LiuModified(nullModel, x.asInstanceOf[Encode.Rare], method)
    }
  }

  def getParameters(p0sqrtZ: DM[Double],
                    rs: Array[Double],
                    pi: Option[DV[Double]] = None,
                    resampled: Option[DM[Double]] = None) : Option[Parameters] = {
    val numSamples = p0sqrtZ.rows
    val numVars = p0sqrtZ.cols
    val meanZ: DV[Double] = sum(p0sqrtZ(*, ::))/numVars.toDouble
    val meanZmat: DM[Double] = DM.zeros[Double](numSamples, numVars)
    meanZmat(::, *) := meanZ

    //val cof1 = (meanZ.t * meanZmat).t/sum(pow(meanZ, 2))
    val cof1 = (meanZ.t * p0sqrtZ).t / sum(pow(meanZ, 2))

    val iterm1 = meanZmat * diag(cof1)
    val iterm2 = p0sqrtZ - iterm1
    /** w3 is the mixture chisq term
      * */
    val w3 = iterm2.t * iterm2

    val varZeta = sum((iterm1.t * iterm1) :* w3) * 4
    val moments: Option[(Double, Double, Double, DV[Double])] =
      if (resampled.isEmpty) {
        SKAT.getLambda(w3).map{l =>
            val m = sum(l)
            val v = 2 * sum(pow(l, 2)) + varZeta
            val k = sum(pow(l, 4))/sum(pow(l, 2)).square * 12
            (m,v,k, l)
        }
      } else {
        SKAT.getLambdaU(w3).map{
          case (l, u) =>
            val m = sum(l)
            val qTmp = pow(resampled.get * iterm2, 2)
            val qs = sum(qTmp(*, ::))
            val dis = new LCCSResampling(l, u, pi.get, qs)
            (m, dis.varQ + varZeta, dis.kurQ, l)
        }
      }
    moments.map{
      case (m, v, k, l) =>
        val sumCof2 = sum(pow(cof1, 2))
        val meanZ2 = sum(pow(meanZ, 2))
        lazy val taus = rs.map{r =>
          meanZ2 * (numVars.toDouble.square * r + (1 - r) * sumCof2)}
        Parameters(m, v, k, l, varZeta, taus)
    }
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
    val informationInverse: DM[Double] = inv(information)
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
    def df: Double = 12.0/kurQ

    override def toString: String = {
      s"""muQ: $muQ
         |varQ: $varQ
         |kurQ: $kurQ
         |lambda: ${lambda.toArray.mkString(",")}
         |varZeta: $varZeta
         |taus: ${taus.mkString(",")}
       """.stripMargin
    }
  }

  final case class Moments(muQ: Double, varQ: Double, df: Double)

  trait AsymptoticKur extends SKATO {

    lazy val paramOpt = getParameters(P0SqrtZ, rhos)

    lazy val lambdasOpt: Option[Array[DV[Double]]] = {
      val res = vcs.map(vc => SKAT.getLambda(vc))
      if (res.exists(_.isEmpty)) {
        None
      } else {
        Some(res.map(_.get))
      }
    }

    def isDefined = paramOpt.isDefined && lambdasOpt.isDefined

    def lambdas = lambdasOpt.get

    def pValues = {

      lambdas.zip(qScores).map{case (l, q) =>
        1.0 - LCCSLiu.Modified(l).cdf(q).pvalue
      }
      /**
        * The Davies method implementation here is bugy
        * use the liu modified instead
        *
      val cdf = lambdas.zip(qScores).map{case (l, q) =>
        (l, q, LCCSDavies.Simple(l).cdf(q))}
      for ((l, q, c) <- cdf) yield
        if (c.pvalue < 0.0 || c.pvalue >1.0) {
          1.0 - LCCSLiu.Modified(l).cdf(q).pvalue
        } else {
          1.0 - c.pvalue
        }
        */
    }
    def pMinQuantiles = {
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
                    x: Encode.Rare,
                    method: String) extends SKATO with AsymptoticKur {
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
    def pValue: Option[Double] = {

      if (isDefined) {
        val re = quadrature(integralFunc, 1e-10, 40.0 + 1e-10)
        re.map(1.0 - _)
      } else {
        None
      }
    }
  }

  @SerialVersionUID(7727760201L)
  trait LiuPValue extends SKATO {

    lazy val term1 = new ChiSquared(df)
    lazy val term2 = new ChiSquared(1)
    lazy val tauDV = DV(param.taus: _*)
    lazy val pmqDV = DV(pMinQuantiles: _*)
    lazy val rDV = DV(rhos.map(1.0 - _): _*)

    def integralFunc(x: Double): Double = {
      val tmp1 = tauDV * x
      val tmp = (pmqDV - tmp1) :/ rDV
      val tmpMin = min(tmp)
      val tmpQ = (tmpMin - param.muQ)/param.varQ.sqrt * (2 * df).sqrt + df
      term1.cdf(tmpQ) * term2.pdf(x)
    }

    def df1pdf(x: DV[Double]): DV[Double] = {
      (pow(x, -0.5) :* exp(-x/2.0))/(2.sqrt * exp(lgamma(0.5)))
    }
    def dfcdf(x: DV[Double]): DV[Double] = {
      try{
        gammp(df/2, x/2.0)
      } catch {
        case e: Exception =>
          val xs = x.toArray.mkString(",")
          println(s"error: param: ${param.toString}")
          DV[Double](s"param: ${param.toString}".toDouble)
      }

    }

    def integralFunc2(x: DV[Double]): DV[Double] = {
      val tmp1: DM[Double] = tile(tauDV, 1, x.length) * diag(x)
      val tmp: DM[Double] = (tile(pmqDV, 1, x.length) - tmp1) :/ tile(rDV, 1, x.length)
      val tmpMin: DV[Double] = min(tmp(::, *)).t
      val tmpQ: DV[Double] = (tmpMin - param.muQ)/param.varQ.sqrt * (2 * df).sqrt + df
      (dfcdf(tmpQ) :* df1pdf(x)).map(i => if (i.isNaN || i < 0.0) 0.0 else i)
    }

    /** adaptive pvalue
      * integration is a little bit heavy here
      * */
    def pValue: Option[Double] = {
      if (isDefined) {
        var continue: Boolean = true
        var i: Int = 0
        var last: Option[Double] = None
        var cur: Option[Double] = None
        while (continue) {
          //println(s"here we go: $i")
          cur = quadratureScan(integralFunc2, 1e-6, 40.0 + 1e-6, 320, (i + 1) * 1000, 1e-5 * pow(10, -i * 2))
          continue =
            cur match {
              case None =>
                cur = last
                false //if no pvalue,
              case Some(v) =>
                last = cur
                (1.0 - v) < 1e-4 * pow(10, -i * 2)
            }
          i += 1
        }
        cur.map(1.0 - _)
      } else {
        None
      }
    }

  }

  @SerialVersionUID(7727760301L)
  case class LiuModified(nullModel: NullModel,
                         x: Encode.Rare,
                         method: String) extends LiuPValue with AsymptoticKur

  {
    //lazy val kurQ = {
    //  12.0 * sum(pow(param.lambda, 4))/sum(pow(param.lambda, 2)).square
    //}
  }

  case class SmallSampleAdjust(nullModel: LogisticModel,
                               x: Encode.Rare,
                               resampled: DM[Double],
                               method: String) extends LiuPValue {

    lazy val paramOpt = getParameters(P0SqrtZ, rhos, Some(nullModel.variance), Some(resampled))

    lazy val lambdasUsOpt = {
      val res = vcs.map { vc => SKAT.getLambdaU(vc) }
      if (res.exists(_.isEmpty)) {
        None
      } else {
        Some((res.map(_.get._1), res.map(_.get._2)))
      }
    }

    def isDefined = paramOpt.isDefined && lambdasUsOpt.isDefined

    lazy val lambdas = lambdasUsOpt.get._1
    lazy val us = lambdasUsOpt.get._2



    lazy val simScores = resampled * geno


    lazy val pValues = {
      rhos.indices.map{i =>
        val simQs = simScores(*, ::).map(s => s.t * kernels(i) * s)
        1.0 - new LCCSResampling(lambdas(i), us(i), nullModel.variance, simQs).cdf(qScores(i)).pvalue
      }.toArray
    }

    lazy val pMinQuantiles = {
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
  def x: Encode.Rare
  def geno: CM[Double] = x.coding

  def numVars: Int = x.vars.length
  //lazy val misc = x.config.misc
  def method: String
  lazy val rhos: Array[Double] = {
    method match {
      case "optimal.adj" => RhosAdj
      case _ => RhosOld
      //case _ => misc.rCorr
    }
  }
  /**
    * we don't store P0 or P0sqrt, because n x n matrix could be too large
    * instead, we always directly compute and store P0sqrt * Z
    *
    *  */
  lazy val P0SqrtZ: DM[Double] = {
    val z: CM[Double] = geno
    nullModel match {
      case lm: LinearModel =>
        (- lm.xsInfoInv * (lm.xs.t * z) + z)/lm.sigma
      case lm: LogisticModel =>
        colMultiply(z, lm.sigma).toDense - lm.xsInfoInv * (lm.xs.t * colMultiply(z, lm.variance))
    }
  }

  def paramOpt: Option[Parameters]

  def param: Parameters = paramOpt.get

  def df = param.df

  lazy val scoreTest: ScoreTest = ScoreTest(nullModel.STNullModel, geno)

  lazy val score = scoreTest.score

  lazy val kernels: Array[DM[Double]] = {
    val i = DM.eye[Double](numVars)
    val o = DM.ones[Double](numVars, numVars)
    rhos.map(r => (1 - r) * i + r * o)
  }

  lazy val LTs: Array[DM[Double]] = kernels.map(cholesky(_).t)


  lazy val qScores: Array[Double] = kernels.map(k => score.t * k * score)

  lazy val vcs = LTs.map(lt => lt.t * P0SqrtZ.t * P0SqrtZ * lt)

  def isDefined: Boolean

  def pValues: Array[Double]

  def pMin = min(pValues)

  def pMinQuantiles: Array[Double]

  def pValue: Option[Double]

  def result = {
    val vs = x.vars
    if (isDefined) {
      AssocMethod.AnalyticResult(vs, pMin, pValue)
    } else {
      AssocMethod.AnalyticResult(vs, -1.0, None)
    }
  }

}

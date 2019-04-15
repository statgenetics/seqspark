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

import breeze.numerics._
import breeze.linalg.{CSCMatrix => CM, DenseMatrix => DM, DenseVector => DV, _}
import breeze.numerics.{lgamma, pow}
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.assoc.SKATO._
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.numerics.Integrate
import org.slf4j.LoggerFactory

import scala.language.existentials

/**
  * optimal SKAT test
  */

object SKATO {
  val logger = LoggerFactory.getLogger(getClass)
  val RhosOld = (0 to 9).map(x => x * 1.0/10.0).toArray :+ 0.999
  val RhosAdj = Array(0.0, 0.01, 0.04, 0.09, 0.16, 0.25, 0.5, 0.999)
  /** the GK integration size */
  val GKSize: Int = 21

  def apply(nullModel: NM,
            x: Encode.Coding,
            method: String): SKATO = {
    val nmf = nullModel match {
      case NM.Simple(y, b) => NM.Fit(y, b)
      case NM.Mutiple(y, c, b) => NM.Fit(y, c, b)
      case nm: NM.Fitted => nm
    }
    method match {
      case "liu"|"liu.mod"|"optimal.moment"|"optimal.moment.adj" =>
        LiuModified(nmf, x.asInstanceOf[Encode.Rare], method)
      case _ =>
        Davies(nmf, x.asInstanceOf[Encode.Rare], method)

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
    /**
      * w3 is the mixture chisq term
      * */
    val w3 = iterm2.t * iterm2

    val varZeta = sum((iterm1.t * iterm1) *:* w3) * 4
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
      method match {
        case "optimal.mod"|"optimal.adj"|"optimal.moment.adj" =>
          lambdas.zip(qScores).map{
            case (l, q) =>
              val cdf = LCCSDavies.Simple(l).cdf(q)
              if (cdf.pvalue >= 1.0 || cdf.pvalue <= 0.0) {
                1.0 - LCCSLiu.Modified(l).cdf(q).pvalue
              } else {
                1.0 - cdf.pvalue
              }
          }
        case _ =>
          lambdas.zip(qScores).map{case (l, q) =>
            1.0 - LCCSLiu.Modified(l).cdf(q).pvalue
          }
      }
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
  case class Davies(nullModel: NM.Fitted,
                    x: Encode.Rare,
                    method: String) extends SKATO with AsymptoticKur {

    def integrand(x: DV[Double]): DV[Double] = {
      require(x.length == GKSize)
      val tmp = (pmqDM - (tauDM(*, ::) *:* x)) /:/ rhoDM
      val kappa = min(tmp(::, *)).t
      val F = kappa.map{k =>
        if (k > sum(param.lambda) * 1e4) {
          1.0
        } else {
          val cutoff = (k - param.muQ) * (param.varQ - param.varZeta).sqrt/param.varQ.sqrt + param.muQ
          LCCSDavies.Simple(param.lambda).cdf(cutoff).pvalue
        }
      }
      //logger.info(s"F: $F")
      F *:* df1pdf(x)
    }

  }

  @SerialVersionUID(7727760201L)
  trait LiuPValue extends SKATO {

    /**
    def integralFunc(x: Double): Double = {
      val tmp1 = tauDV * x
      val tmp = (pmqDV - tmp1) :/ rDV
      val tmpMin = min(tmp)
      val tmpQ = (tmpMin - param.muQ)/param.varQ.sqrt * (2 * df).sqrt + df
      term1.cdf(tmpQ) * term2.pdf(x)
    }
    */


    def integrand(x: DV[Double]): DV[Double] = {
      require(x.length == GKSize)
      val tmp = (pmqDM - (tauDM(*, ::) *:* x)) /:/ rhoDM
      val kappa = min(tmp(::, *)).t
      //val cutoff = (kappa - param.muQ) * (param.varQ - param.varZeta).sqrt/param.varQ.sqrt + param.muQ
      val cutoff = (kappa - param.muQ)/param.varQ.sqrt * (2 * df).sqrt + df
      //logger.debug(s"x: $x")
      //logger.debug(s"tmpMin: $tmpMin")
      //logger.debug(s"tmpQ: $tmpQ")
      /**
      if (tmpQ.exists(_.isInfinity)) {
        (s"x:${x.toArray.mkString(",")}\n" +
          s"lambdas: ${lambdas.map(_.toArray.mkString(",")).mkString("\n")}\n" +
          s"lambdas2: ${lambdas2.map(_.toArray.mkString(",")).mkString("\n")}\n" +
          s"qscores: ${qScores.mkString(",")}\n" +
          s"pvalues: ${pValues.mkString(",")}\n" +
          s"pmqDV: ${pmqDV.toArray.mkString(",")}\n" +
          s"rDV: ${rDV.toArray.mkString(",")}\n" +
          s"tauDV: ${tauDV.toArray.mkString(",")}\n" +
          s"tmpMin: ${tmpMin.toArray.mkString(",")}\n" +
          s"param: ${param.toString}\n" +
          s"tmpQ: ${tmpQ.toArray.mkString(",")}\n").toDouble
      }
        */
      val res = dfcdf(cutoff.map(q => if (q < 0) 0.0 else q)) *:* df1pdf(x)
      //logger.debug(s"res: $res")
      res
    }


  }

  @SerialVersionUID(7727760301L)
  case class LiuModified(nullModel: NM.Fitted,
                         x: Encode.Rare,
                         method: String)
    extends LiuPValue with AsymptoticKur

  case class SmallSampleAdjust(nullModel: NM.Fitted,
                               x: Encode.Rare,
                               resampled: DM[Double],
                               method: String)
    extends LiuPValue
  {
    lazy val paramOpt = getParameters(P0SqrtZ, rhos, Some(nullModel.b), Some(resampled))

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
        1.0 - new LCCSResampling(lambdas(i), us(i), nullModel.b, simQs).cdf(qScores(i)).pvalue
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
  def nullModel: NM.Fitted
  def x: Encode.Rare
  def geno: CM[Double] = x.coding

  def numVars: Int = x.coding.cols
  //lazy val misc = x.config.misc
  def method: String
  lazy val rhos: Array[Double] = {
    method match {
      case "optimal.adj" => RhosAdj
      case "optimal" => RhosOld
      case _ => RhosAdj
      //case _ => misc.rCorr
    }
  }

  lazy val P0SqrtZ: DM[Double] = {
    val z: CM[Double] = geno
    val nm = nullModel
    val sigma = sqrt(nm.b)
    val xsInfoInv = (nm.xs(::, *) *:* sigma) * nm.invInfo * nm.a
    (- xsInfoInv * (nm.xs.t * colMultiply(z, nm.b)) + colMultiply(z, sigma)) / sqrt(nm.a)
  }

  def paramOpt: Option[Parameters]

  def param: Parameters = paramOpt.get

  def df = param.df

  lazy val scoreTest: ScoreTest = ScoreTest(nullModel, geno)

  lazy val score = scoreTest.score

  lazy val kernels: Array[DM[Double]] = {
    val i = DM.eye[Double](numVars)
    val o = DM.ones[Double](numVars, numVars)
    rhos.map(r => (1 - r) * i + r * o)
  }

  lazy val LTs: Array[DM[Double]] = kernels.map(x => cholesky(x))


  lazy val qScores: Array[Double] = kernels.map(k => score.t * k * score)

  lazy val P0Z = P0SqrtZ.t * P0SqrtZ

  lazy val vcs = LTs.map(lt => lt.t * P0Z * lt)

  /**
  lazy val vcs2 = kernels.map{k =>
    ScoreTest(nullModel.STNullModel, geno * cholesky(k).t).variance
  }

  lazy val lambdas2 = vcs2.map(vc =>
    eigSym.justEigenvalues(vc)
  )
  */

  def isDefined: Boolean

  def pValues: Array[Double]

  def pMin = min(pValues)

  def lambdas: Array[DV[Double]]

  def pMinQuantiles: Array[Double]

  def result: AssocMethod.SKATOResult = {
    if (isDefined) {
      try {
        val res = Integrate(integrand, 0.0, 40.0, 1e-25, 1e-4, 200)
        val info = s"pvalues=${pValues.mkString(",")};abserr=${res.abserr};ier=${res.iEr};nsub=${res.nSub};neval=${res.nEval}"
        AssocMethod.SKATOResult(x.vars, Some(pMin), Some(1 - res.value), info)
      } catch {
        case e: Exception =>
          val info = s"InegrationError;pvalues=${pValues.mkString(",")}"
          AssocMethod.SKATOResult(x.vars, Some(pMin), None, info)
      }
    } else {
      AssocMethod.SKATOResult(x.vars, None, None, "failed to get the p values")
    }
  }

  /**
    * help variables for the integrand function
    * basically
    *
    * */
  //lazy val term1 = new ChiSquared(df)
  //lazy val term2 = new ChiSquared(1)
  //lazy val tauDV = DV(param.taus)
  //lazy val pmqDV = DV(pMinQuantiles)
  //lazy val rDV = DV(rhos.map(1.0 - _))
  lazy val tauDM = tile(DV(param.taus), 1, GKSize)
  lazy val pmqDM = tile(DV(pMinQuantiles), 1, GKSize)
  lazy val rhoDM = tile(DV(rhos.map(1.0 - _)), 1, GKSize)

  def df1pdf(x: DV[Double]): DV[Double] = {
    (pow(x, -0.5) *:* exp(-x/2.0))/(2.sqrt * exp(lgamma(0.5)))
  }

  def dfcdf(x: DV[Double]): DV[Double] = {
    try {
      gammp(df/2, x/2.0)
    } catch {
      case e: Exception =>
        val xs = x.toArray.mkString(",")
        println(s"error: param: ${param.toString}")
        DV[Double](s"param: ${param.toString}".toDouble)
    }
  }

  def integrand(x: DV[Double]): DV[Double]

  /** adaptive pvalue
    * use the quadpack QAGS now
  def pValue: Option[Double] = {
    if (isDefined) {
      val res = Integrate(integrand, 0.0, 40.0, 1e-25, 1e-6, 200)
      if (res.iEr == 0) {
        Some(1.0 - res.value)
      } else {
        None
      }
    } else {
      None
    }
  }
    */

}

package org.dizhang.seqspark.assoc

import breeze.linalg.{*, diag, eig, min, sum, trace, DenseMatrix => DM, DenseVector => DV}
import org.dizhang.seqspark.ds.{SafeMatrix => SM}
import SKATO._
import breeze.numerics.pow
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.stat.{LCCSDavies, LCCSLiu}
import org.dizhang.seqspark.util.General._
import collection.JavaConverters._
import breeze.integrate._

/**
  * optimal skat
  */

object SKATO {
  class One //number one
  class N //number of samples
  class MC //number of covariates
  class MC1 //nuber of covariates plus one
  class MG //number of variants
  val NumRhos = 10
  val RhosOld = (0 to NumRhos).map(x => x * 1.0/NumRhos).toArray
  val RhosAdj = Array(0.0, 0.01, 0.04, 0.09, 0.16, 0.25, 0.5, 1.0)


  /** Compute the Kurtosis for the generalized SKAT statistic Q(rho)
    * (1 - rho) * kapp1 + tau(rho) * kappa2
    *
    * kappa1 ~ LCCS(lambda, df = Ones) + Zeta
    * kappa2 ~ ChiSquared(df = 1)
    * */
  def getKurtosis(df1: Double, // df of kappa1
                  df2: Double, // df of kappa2, usually 1.0
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
  final case class Moments(muQ: Double, varQ: Double, df: Double)


  case class Davies(y: DV[Double],
                    yEstimate: DV[Double],
                    x: Encode,
                    p0sqrt: DM[Double]) extends SKATO {
    lazy val term2 = new ChiSquared(1.0)

    def integralFunc(x: Double): Double = {
      val tmp1 = DV(taus: _*) * x
      val tmp = (DV(tQs: _*) - tmp1) :/ DV(rhos.map(1.0 - _): _*)
      val kappa = min(tmp)
      val term1 =
        if (kappa > sum(lambda) * 1e4) {
          1.0
        } else {
          val tmpQ = (kappa - muQ) * (deltaQ.square - deltaZeta.square).sqrt / deltaQ + muQ
          LCCSDavies.Simple(lambda).cdf(tmpQ).pvalue
        }
      term1 * term2.pdf(x)
    }
    def pValue: Double = {
      val re = simpson(integralFunc, 0.0, 40.0, 2000)
      1.0 - re
    }
  }

  trait LiuPValue extends SKATO {
    def kurQ: Double
    def df = 12.0/kurQ
    lazy val term1 = new ChiSquared(df)
    lazy val term2 = new ChiSquared(1.0)

    def integralFunc(x: Double): Double = {
      val tmp1 = DV(taus: _*) * x
      val tmp = (DV(tQs: _*) - tmp1) :/ DV(rhos.map(1.0 - _): _*)
      val tmpMin = min(tmp)
      val tmpQ = (tmpMin - muQ)/deltaQ * (2 * df).sqrt + df
      term1.cdf(tmpQ) * term2.pdf(x)
    }
    def pValue: Double = {
      val re = simpson(integralFunc, 0.0, 40.0, 2000)
      1.0 - re
    }
  }

  case class LiuModified(y: DV[Double],
                         yEstimate: DV[Double],
                         x: Encode,
                         p0sqrt: DM[Double]) extends LiuPValue {
    lazy val kurQ = {
      12.0 * sum(pow(lambda, 4))/sum(pow(lambda, 2)).square
    }
  }


  case class SmallSampleAdjust(y: DV[Double],
                               yEstimate: DV[Double],
                               x: Encode,
                               p0sqrt: DM[Double],
                               resampled: (DM[Double], DM[Double])) extends LiuPValue {
    override lazy val deltaQ = {
      val varQ = SKAT.getVar(lambda, yEstimate, u) + deltaZeta.square
      varQ.sqrt
    }
    lazy val models = {
      rhos.map{ r =>
        SKAT.SmallSampleAdjust(y, yEstimate, x, resampled, r)
      }
    }

    lazy val moments = {
      models.zipWithIndex.map{case (m, i) =>
        val kur = getKurtosis(m.df, 1.0, m.varQ + deltaZeta.square, 1.0 - rhos(i), taus(i))
        val df = 12.0/kur
        Moments(m.muQ, m.varQ, df)}
    }

    override lazy val ps = {
      models.map(m => m.pValue)
    }

    override lazy val tQs = {
      moments.map{m =>
        val origQ = new ChiSquared(m.df).inverseCdf(t)
        (origQ - m.df)/(2 * m.df).sqrt * m.varQ + m.muQ
      }
    }
    lazy val kurQ = {
      val (ys, yEstimates) = resampled
      val residuals = ys - yEstimates
      val simulatedQs = DV((for (i <- 0 until ys.cols)
        yield residuals(::, i).t * matrixZIMZ * residuals(::, i)): _*)
      SKAT.getKurtosis(simulatedQs)
    }
  }
}

trait SKATO extends AssocMethod {
  def y: DV[Double]
  def yEstimate: DV[Double]
  def x: Encode
  def p0sqrt: DM[Double]
  def numSample = y.length
  def numVars = x.weight().get.length
  lazy val misc = x.config.misc
  lazy val method = misc.getString("method")
  lazy val rhos: Array[Double] = {
    val res = method match {
      case "optimal.adj" => RhosAdj
      case "optimal" => RhosOld
      case _ => misc.getDoubleList("rCorr").asScala.toArray.map(_.toDouble)
    }
  }
  lazy val residual = SM[N, One](y - yEstimate)
  lazy val matrixZ = p0sqrt * x.getRare().get.coding * diag(x.weight().get)
  lazy val vectorZ = sum(matrixZ(*, ::))/numVars.toDouble
  lazy val matrixM = {
    val z = vectorZ
    z * (z.t * z) * z.t
  }
  lazy val matrixZIMZ = {
    val z = matrixZ
    val m = matrixM
    val i = DM.eye[Double](m.cols)
    z.t * (i - m) * z
  }
  lazy val qScores = {
    rhos.map(r => (residual.t * kernel(r) * residual).apply(0,0))
  }
  lazy val (lambda, u) = {
    val m = matrixZ.t * (DM.eye[Double](numSample) - matrixM) * matrixZ
    val e = eig(m)
    (e.eigenvalues, e.eigenvectors)
  }
  lazy val muQ = sum(lambda)
  lazy val deltaZeta = {
    val m = matrixM
    val z = matrixZ
    val i = DM.eye[Double](numSample)
    val res = z.t * m * z * z.t * (i - m) * z
    2 * pow(trace(res), 0.5)
  }

  lazy val deltaQ = {
    pow(2 * sum(pow(lambda, 2)) + pow(deltaZeta, 2), 0.5)
  }

  def kernel(rho: Double): SM[N, N] = {
    val g = x.getRare().get.coding
    val w = diag(x.weight().get)
    val mg = w.cols
    val r = (1.0 - rho) * DM.eye[Double](mg) + rho * DM.ones[Double](mg, mg)
    val k = g * w * r * w * g.t
    SM[N, N](k)
  }

  def tau(rho: Double): Double = {
    val prod = vectorZ.t * vectorZ
    val zz = (vectorZ.t * matrixZ).t
    pow(numVars, 2) * prod + (1 - rho)/prod * sum(pow(zz, 2))
  }
  lazy val taus = rhos.map{r => tau(r)}
  lazy val lambdas = rhos.map{r =>
    val kBar = p0sqrt * kernel(r).mat * p0sqrt
    eig(kBar).eigenvalues}

  lazy val ps = {
    val cdf = lambdas.zip(qScores).map{case (l, q) =>
      (l, q, LCCSDavies.Simple(l).cdf(q))}
    for ((l, q, c) <- cdf) yield
      if (c.pvalue < 0.0 || c.pvalue >1.0) {
        LCCSLiu.Modified(l).cdf(q).pvalue
      } else {
        c.pvalue
      }
  }
  lazy val t = min(ps)
  lazy val tQs = {
    lambdas.map{l =>
      val lm = LCCSLiu.Modified(l)
      val muQ = lm.muQ
      val varQ = lm.sigmaQ.square
      val df = lm.l
      val chiDis = new ChiSquared(df)
      val qOrig = chiDis.inverseCdf(1.0 - t)
      (qOrig - df)/(2 * df).sqrt * lm.sigmaQ + muQ
    }
  }
}

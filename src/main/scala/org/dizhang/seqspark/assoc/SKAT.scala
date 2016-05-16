package org.dizhang.seqspark.assoc

import breeze.linalg.{*, diag, eig, sum, svd, DenseMatrix => DM, DenseVector => DV}
import breeze.numerics.{pow, sqrt}
import breeze.stats._
import org.dizhang.seqspark.stat.{LCCSDavies, LCCSLiu, NonCentralChiSquare}
import org.dizhang.seqspark.ds.{SafeMatrix => SM}
import org.dizhang.seqspark.util.General._
/**
  * variance component test
  */

object SKAT {
  class One //number one
  class N //number of samples
  class KC //number of covariates
  class KC1 //nuber of covariates plus one
  class KG //number of variants
  def apply(y: DV[Double],
            yEstimate: DV[Double],
            x: Encode,
            p0sqrt: DM[Double]): SKAT = {
    val method = x.config.misc.getString("method")
    method match {
      case "liu.mod" => LiuModified(y, yEstimate, x, p0sqrt)
      case "liu" => Liu(y, yEstimate, x, p0sqrt)
      case _ => Davies(y, yEstimate, x, p0sqrt)
    }
  }

  def apply(y: DV[Double],
            yEstimate: DV[Double],
            x: Encode,
            resampled: (DM[Double], DM[Double])): SKAT = {
    SmallSampleAdjust(y, yEstimate, x, resampled)
  }

  def nullVariance(isBinary: Boolean,
                   y: DV[Double],
                   yEstimate: DV[Double]): DM[Double] = {
    if (isBinary) {
      diag(yEstimate.map(y => y * (1.0 - y)))
    } else {
      val sigma2 = mean(pow(y - yEstimate, 2.0))
      diag(DV.fill(y.length)(sigma2))
    }
  }
  def matrixSqrt[A](m: SM[A, A]): SM[A, A] = {
    val de = eig(m.mat)
    val evalues = de.eigenvalues
    val evectors = de.eigenvectors
    val res = evectors * diag(sqrt(evalues)) * evectors.t
    SM[A, A](res)
  }
  def matrixP0sqrt(variance: DM[Double],
                   cov: DM[Double]): DM[Double] = {
    val ones = DV.ones[Double](cov.rows)
    val xs = SM[N, KC1](DM.horzcat(ones.toDenseMatrix.t, cov))
    val v = SM[N, N](variance)
    val p0 = v - v * xs * (xs.t * v * xs) * xs.t * v
    matrixSqrt(p0).mat
  }

  def varElement(m4: DV[Double],
                 yEstimate: DV[Double],
                 u1: DV[Double],
                 u2: DV[Double]): Double = {
    val tmp = pow(u1, 2) :* pow(u2, 2)
    val a1 = m4 dot tmp
    val a2 = sum(pow(u1,2)) * sum(pow(u2, 2)) - sum(tmp)
    val a3 = (u1 dot u2).square - sum(tmp)
    a1 + a2 + 2*a3
  }
  def getSkewness(qs: DV[Double]): Double = {
    val m3 = mean(pow(qs - mean(qs), 3))
    m3/stddev(qs).cube
  }

  def getKurtosis(qs: DV[Double]): Double = {

    if (stddev(qs) > 0) {
      val m4 = mean(pow(qs - mean(qs), 4))
      m4 / stddev(qs).square.square - 3
    } else {
      -100.0
    }
  }

  def getDF(qscores: DV[Double]): Double = {
    val s2 = getKurtosis(qscores)
    val df = 12.0/s2
    if (s2 < 0) {
      100000.0
    } else if (df < 0.01) {
      val s1 = getSkewness(qscores)
      8.0/s1.square
    } else {
      df
    }
  }

  def getVar(lambda: DV[Double], yEstimate: DV[Double], u: DM[Double]): Double = {
    val n = lambda.length
    val m4 = yEstimate.map(p =>
      p * (1.0 - p) * (3 * p.square - 3 * p + 1) / (p * (1.0 - p)).square)
    val zeta = DV((
      for {
        i <- 0 until n
        tmp = sum(pow(u(::, i), 2)).square - sum(pow(u(::,i), 4))
      } yield (m4 dot pow(u(::, i), 4)) + 3 * tmp): _*)
    val cov =
      if (n == 1) {
        DM(zeta(0) * lambda(0).square)
      } else {
        diag(zeta :* pow(lambda, 2))
      }
    for {
      i <- 0 until (n - 1)
      j <- (i + 1) until n
    } {
      cov(i,j) = varElement(m4, yEstimate, u(::,i), u(::,j))
      cov(i,j) *= lambda(i) * lambda(j)
      cov(j,i) = cov(i,j)
    }
    sum(cov) - sum(lambda).square
  }

  final case class Davies(y: DV[Double],
                          yEstimate: DV[Double],
                          x: Encode,
                          p0sqrt: DM[Double],
                          rho: Double = 0.0) extends SKAT {
    def pValue: Double = {
      val tmp = p0sqrt * kernel * p0sqrt
      val lambda = eig(tmp).eigenvalues
      1.0 - LCCSDavies.Simple(lambda).cdf(qScore).pvalue
    }
  }
  final case class Liu(y: DV[Double],
                       yEstimate: DV[Double],
                       x: Encode,
                       p0sqrt: DM[Double],
                       rho: Double = 0.0) extends SKAT {
    def pValue: Double = {
      val tmp = p0sqrt * kernel * p0sqrt
      val lambda = eig(tmp).eigenvalues
      1.0 - LCCSLiu.Simple(lambda).cdf(qScore).pvalue
    }
  }
  final case class LiuModified(y: DV[Double],
                               yEstimate: DV[Double],
                               x: Encode,
                               p0sqrt: DM[Double],
                               rho: Double = 0.0) extends SKAT {
    def pValue: Double = {
      val tmp = p0sqrt * kernel * p0sqrt
      val lambda = eig(tmp).eigenvalues
      1.0 - LCCSLiu.Modified(lambda).cdf(qScore).pvalue
    }
  }
  final case class SmallSampleAdjust(y: DV[Double],
                                     yEstimate: DV[Double],
                                     x: Encode,
                                     resampled: (DM[Double], DM[Double]),
                                     rho: Double = 0.0) extends SKAT {
    val (ys, yEstimates) = resampled
    val resampleNum = ys.cols
    val variances: DM[Double] =  -(yEstimates(::, *) - DV.ones[Double](y.length)) :* yEstimates
    val residuals = ys - yEstimates
    val qs: DV[Double] = DV((for (i <- 0 until resampleNum)
      yield residuals(::, i).t * kernel * residuals(::, i)): _*)
    val (lambda, u) = {
      val variance = diag(yEstimate :* (DV.ones[Double](y.length) - yEstimate))
      val vs = matrixSqrt(SM[N, N](variance)).mat
      val kBar = vs * kernel * vs
      val eigen = eig(kBar)
      val (values, vectors) = (eigen.eigenvalues, eigen.eigenvectors)
      val indices = values.toArray.zipWithIndex.filter(p => p._1.square > 1e-5).map(_._2)
      val lb = DV(values.toArray.filter(p => p.square > 1e-5): _*)
      val vec = DV.horzcat((for (i <- indices) yield vectors(::, i)): _*)
      (lb, vec)
    }
    val muQ = sum(lambda)
    val varQ = getVar(lambda, yEstimate, u)
    val df = getDF(qs)
    def pValue: Double = {
      val norm = (qScore - muQ)/varQ.sqrt
      val norm1 = norm * (2 * df).sqrt + df
      1.0 - NonCentralChiSquare(df, 0.0).cdf(norm1)
    }
  }

}

trait SKAT extends AssocMethod {
  def y: DV[Double]
  def yEstimate: DV[Double]
  def x: Encode
  def isDefined: Boolean = x.isDefined
  def rho: Double
  lazy val residual: DV[Double] = y - yEstimate
  lazy val kernel: DM[Double] = {
    val weight = diag(x.weight().get)
    val g = x.getRare().get.coding
    val r = (1.0 - rho) * DM.eye[Double](weight.cols) + rho * DM.ones[Double](weight.rows, weight.cols)
    g * weight * r * weight * g.t
  }
  def qScore: Double = {
    residual.t * kernel * residual
  }
  def pValue: Double
}

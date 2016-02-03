package org.dizhang.seqspark.stat

import breeze.linalg.DenseVector
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.{FDistribution, Gaussian}
import org.dizhang.seqspark.assoc.Encode.{VT, Fixed, Coding}
import org.dizhang.seqspark.ds.SafeMatrix
import org.dizhang.seqspark.stat.ScoreTest._

/**
  * Score test for regression
  * Why some case classes take estimates as input, instead of computing one?
  * some tests share the same estimates, so we can compute it once and pass
  * it to all the ScoreTests
  */

object ScoreTest {
  /** the following three classes are fake dimensions */
  class One //1
  class N //number of samples
  class K //number of features
  class S //number of schemes (weight or maf cutoff)

  def ones(n: Int) = DenseVector.ones(n)

  def apply(binaryTrait: Boolean,
            y: DenseVector[Double],
            x: Coding,
            cov: Option[DenseMatrix[Double]] = None,
            estimates: Option[DenseVector[Double]] = None): ScoreTest = {
    x match {
      case Fixed(c) => apply(binaryTrait, y, c, cov, estimates)
      case VT(c) => apply(binaryTrait, y, c, cov.get, estimates.get)
    }
  }

  def apply(binary: Boolean,
            y: DenseVector[Double],
            x: DenseVector[Double],
            cov: Option[DenseMatrix[Double]] = None,
            estimates: Option[DenseVector[Double]] = None): ScoreTest = {
    cov match {
      case None => if (binary) BinaryNoCovUniScoreTest(y, x) else QuantNoCovUniScoreTest(y, x)
      case Some(c) => if (binary) BinaryCovUniScoreTest(y, x, c, estimates.get) else QuantCovUniScoreTest(y, x, c, estimates.get)
    }
  }

  def apply(binary: Boolean,
            y: DenseVector[Double],
            x: DenseMatrix[Double],
            cov: DenseMatrix[Double],
            estimates: DenseVector[Double]): ScoreTest = {
    if (binary)
      BinaryCovMultiScoreTest(y, x, cov, estimates)
    else
      QuantCovMultiScoreTest(y, x, cov, estimates)
  }
}

sealed trait ScoreTest extends HypoTest {
  val u: Double
  val v: Double
  lazy val t = u / pow(v, 0.5)
  lazy val statistic: Double = t
  lazy val pValue: Double = {
    val dis = new Gaussian(0, 1)
    /** single side */
    dis.cdf(- abs(t))
  }

  def summary: TestResult =
    new TestResult {
      override val estimate: Option[Double] = None

      override val stdErr: Option[Double] = None

      override val pValue: Double = pValue

      override val statistic: Double = statistic
    }
}

case class BinaryNoCovUniScoreTest(y: DenseVector[Double],
                                   x: DenseVector[Double]) extends ScoreTest {
  lazy val p: Double = mean(y)
  lazy val n: Int = y.length
  lazy val u: Double = y.map(_ - p).t * x
  lazy val v: Double = p * (1 - p) * (x.t * x - 1 / n * pow(sum(x), 2))
}

case class BinaryCovUniScoreTest(y: DenseVector[Double],
                                 x: DenseVector[Double],
                                 cov: DenseMatrix[Double],
                                 estimates: DenseVector[Double]) extends ScoreTest {
  lazy val n = y.length
  lazy val xs = DenseMatrix.horzcat(DenseVector.ones(n).toDenseMatrix.t, cov)
  lazy val mp: DenseVector[Double] = estimates
  lazy val vp: DenseVector[Double] = mp :* mp.map(1 - _)
  lazy val u: Double = (y - mp).t * x
  lazy val v: Double = {
    val tmp1 = SafeMatrix[N, One](vp :* x).t * SafeMatrix[N, K](xs)
    val tmp2 = SafeMatrix.inverse(SafeMatrix[K, N](xs.t) * SafeMatrix[N, N](diag(vp)) * SafeMatrix[N, K](xs))
    sum(vp :* x :* x) - (tmp1 * tmp2 * tmp1.t).mat(0,0)
  }
}

case class BinaryCovMultiScoreTest(y: DenseVector[Double],
                                   x: DenseMatrix[Double],
                                   cov: DenseMatrix[Double],
                                   estimates: DenseVector[Double]) extends ScoreTest {
  /** For K choices of weight schemes */
  lazy val u = 0
  lazy val v = 1
  lazy val n = y.length
  lazy val k = cov.cols + 1
  lazy val s = x.cols
  lazy val xs = DenseMatrix.horzcat(ones(n).toDenseMatrix.t, cov)
  lazy val mp: DenseVector[Double] = estimates
  lazy val vp: DenseVector[Double] = mp :* mp.map(1 - _)
  lazy val uVec: DenseVector[Double] = (SafeMatrix[N, S](x).t * SafeMatrix[N, One](y - mp)).mat(::, 0)
  lazy val tmp0 = SafeMatrix[N, One](vp).t * SafeMatrix[N, S](x :* x)
  lazy val tmp1 = SafeMatrix[N, S](x).t * SafeMatrix[N, N](diag(vp)) * SafeMatrix[N, K](xs)
  lazy val tmp2 = SafeMatrix.inverse(SafeMatrix[K, N](xs.t) * SafeMatrix[N, N](diag(vp)) * SafeMatrix[N, K](xs))
  lazy val vVec: DenseVector[Double] = tmp0.mat(0, ::).t - diag((tmp1 * tmp2 * tmp1.t).mat)
  lazy val tVec = uVec :/ pow(vVec, 0.5)
  lazy val tMax = max(tVec)
  override lazy val statistic = {
    val tmp = SafeMatrix[N, N](diag(y - mp)) * (SafeMatrix[N, S](x) - SafeMatrix[N, K](xs) * tmp2.t * tmp1.t)
    val covU = (tmp.t * tmp).mat
    val dU = diag(pow(diag(covU), 0.5))
    val covT = SafeMatrix[S, S](dU) * SafeMatrix[S, S](covU) * SafeMatrix[S, S](dU)
    val tMaxVec = DenseVector.fill(covT.mat.cols)(this.tMax)
    val t2 = y.length * (SafeMatrix[S, One](tMaxVec).t * SafeMatrix.inverse(covT) * SafeMatrix[S, One](tMaxVec)).mat(0, 0)
    val f = (y.length - covU.cols)/(covU.cols * (y.length - 1)) * t2
    f
  }
  override lazy val pValue = 1 - new FDistribution(s, n - s).cdf(statistic)
}

case class QuantNoCovUniScoreTest(y: DenseVector[Double], x: DenseVector[Double]) extends ScoreTest {
  lazy val my: Double = mean(y)
  lazy val vy: Double = variance(y)
  lazy val n: Int = y.length
  lazy val u: Double = y.map(_ - my).t * x
  lazy val v: Double = vy * ( x.t * x - 1/n * pow(sum(x), 2))
}

case class QuantCovUniScoreTest(y: DenseVector[Double],
                                x: DenseVector[Double],
                                cov: DenseMatrix[Double],
                                estimates: DenseVector[Double]) extends ScoreTest {
  lazy val n = y.length
  lazy val xs = DenseMatrix.horzcat(ones(n).toDenseMatrix.t, cov)
  lazy val my: DenseVector[Double] = estimates
  lazy val vy: Double = 1/n * pow(norm(y - my), 2)
  lazy val u: Double = (y - my).t * x
  lazy val v: Double = {
    val tmp1 = SafeMatrix[N, One](x).t * SafeMatrix[N, K](xs)
    val tmp2 = SafeMatrix.inverse(SafeMatrix[K, N](xs.t) * SafeMatrix[N, K](xs))
    vy * (pow(norm(x), 2) - (tmp1 * tmp2 * tmp1.t).mat(0, 0))
  }
}

case class QuantCovMultiScoreTest(y: DenseVector[Double],
                                  x: DenseMatrix[Double],
                                  cov: DenseMatrix[Double],
                                  estimates: DenseVector[Double]) extends ScoreTest {
  lazy val u = 0
  lazy val v = 0
  lazy val n = y.length
  lazy val s = x.cols
  lazy val xs = DenseMatrix.horzcat(ones(n).toDenseMatrix.t, cov)
  lazy val my: DenseVector[Double] = estimates
  lazy val vy: Double = 1/n * pow(norm(y - my), 2)
  lazy val uVec: DenseVector[Double] = (SafeMatrix[N, S](x).t * SafeMatrix[N, One](y - my)).mat(::, 0)
  private lazy val tmp0 = SafeMatrix[N, One](DenseVector.fill(n)(1.0)).t * SafeMatrix[N, S](x :* x)
  private lazy val tmp1 = SafeMatrix[N, S](x).t * SafeMatrix[N, K](xs)
  private lazy val tmp2 = SafeMatrix.inverse(SafeMatrix[K, N](xs.t) * SafeMatrix[N, K](xs))
  lazy val vVec = vy * (tmp0.mat(0, ::).t - diag((tmp1 * tmp2 * tmp1.t).mat))
  lazy val tVec = uVec :/ pow(vVec, 0.5)
  lazy val tMax = max(tVec)
  override lazy val statistic = {
    val tmp = SafeMatrix[N, N](diag(y - my)) * (SafeMatrix[N, S](x) - SafeMatrix[N, K](xs) * tmp2.t * tmp1.t)
    val covU = (tmp.t * tmp).mat
    val dU = diag(pow(diag(covU), 0.5))
    val covT = SafeMatrix[S, S](dU) * SafeMatrix[S, S](covU) * SafeMatrix[S, S](dU)
    val tMaxVec = DenseVector.fill(s)(this.tMax)
    val t2 = n * (SafeMatrix[S, One](tMaxVec).t * SafeMatrix.inverse(covT) * SafeMatrix[S, One](tMaxVec)).mat(0, 0)
    val f = (n - s)/(s * (n - 1)) * t2
    f
  }
  override lazy val pValue = 1 - new FDistribution(s, n - s).cdf(statistic)
}
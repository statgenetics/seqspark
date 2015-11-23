package org.dizhang.seqa.stat

import breeze.linalg.DenseVector
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.{FDistribution, Gaussian}
import org.dizhang.seqa.ds.MyMatrix
import org.dizhang.seqa.stat.ScoreTest._


/**
  * Score test for regression
  */
object ScoreTest {
  /** the following three classes are fake dimensions */
  class One //1
  class N //number of samples
  class K //number of features
  class S //number of schemes (weight or maf cutoff)

  def apply(binary: Boolean,
            y: DenseVector[Double],
            x: DenseVector[Double],
            cov: Option[DenseMatrix[Double]] = None): ScoreTest = {
    cov match {
      case None => if (binary) BinaryNoCovUniScoreTest(y, x) else QuantNoCovUniScoreTest(y, x)
      case Some(c) => if (binary) BinaryCovUniScoreTest(y, x, c) else QuantCovUniScoreTest(y, x, c)
    }
  }

  def apply(binary: Boolean,
            y: DenseVector[Double],
            x: DenseMatrix[Double],
            cov: DenseMatrix[Double] = None): ScoreTest = {
    if (binary)
      BinaryCovMultiScoreTest(y, x, cov)
    else
      QuantCovMultiScoreTest(y, x, cov)
  }
}

sealed trait ScoreTest {
  val u: Double
  val v: Double
  lazy val t = u / pow(v, 0.5)
  lazy val pValue: Double = {
    val dis = new Gaussian(0, 1)
    /** single side */
    dis.cdf(- abs(t))
  }
}

case class BinaryNoCovUniScoreTest(y: DenseVector[Double], x: DenseVector[Double]) extends ScoreTest {
  lazy val p: Double = mean(y)
  lazy val n: Int = y.length
  lazy val u: Double = y.map(_ - p).t * x
  lazy val v: Double = p * (1 - p) * (x.t * x - 1 / n * pow(sum(x), 2))
}

case class BinaryCovUniScoreTest(y: DenseVector[Double], x: DenseVector[Double], cov: DenseMatrix[Double]) extends ScoreTest {
  lazy val baseModel = new LogisticRegression(y, cov)
  lazy val mp: DenseVector[Double] = baseModel.estimates
  lazy val vp: DenseVector[Double] = mp :* mp.map(1 - _)
  lazy val u: Double = (y - mp).t * x
  lazy val v: Double = {
    val tmp1 = MyMatrix[N, One](vp :* x).t * MyMatrix[N, K](baseModel.xs)
    val tmp2 = MyMatrix.inverse(MyMatrix[K, N](baseModel.xs.t) * MyMatrix[N, N](diag(vp)) * MyMatrix[N, K](baseModel.xs))
    sum(vp :* x :* x) - (tmp1 * tmp2 * tmp1.t).mat(0,0)
  }
}

case class BinaryCovMultiScoreTest(y: DenseVector[Double],
                                   x: DenseMatrix[Double],
                                   cov: DenseMatrix[Double]) extends ScoreTest {
  /** For K choices of weight schemes */
  lazy val u = 0
  lazy val v = 1
  lazy val baseModel = new LogisticRegression(y, cov)
  lazy val mp: DenseVector[Double] = baseModel.estimates
  lazy val vp: DenseVector[Double] = mp :* mp.map(1 - _)
  lazy val uVec: DenseVector[Double] = (MyMatrix[N, S](x).t * MyMatrix[N, One](y - mp)).mat(::, 0)
  lazy val tmp0 = MyMatrix[N, One](vp).t * MyMatrix[N, S](x :* x)
  lazy val tmp1 = MyMatrix[N, S](x).t * MyMatrix[N, N](diag(vp)) * MyMatrix[N, K](baseModel.xs)
  lazy val tmp2 = MyMatrix.inverse(MyMatrix[K, N](baseModel.xs.t) * MyMatrix[N, N](diag(vp)) * MyMatrix[N, K](baseModel.xs))
  lazy val vVec: DenseVector[Double] = tmp0.mat(0, ::).t - diag((tmp1 * tmp2 * tmp1.t).mat)
  lazy val tVec = uVec :/ pow(vVec, 0.5)
  lazy val tMax = max(tVec)
  override lazy val pValue = {
    val tmp = MyMatrix[N, N](diag(y - mp)) * (MyMatrix[N, S](x) - MyMatrix[N, K](baseModel.xs) * tmp2.t * tmp1.t)
    val covU = (tmp.t * tmp).mat
    val dU = diag(pow(diag(covU), 0.5))
    val covT = MyMatrix[S, S](dU) * MyMatrix[S, S](covU) * MyMatrix[S, S](dU)
    val tMaxVec = DenseVector.fill(covT.mat.cols)(this.tMax)
    val t2 = y.length * (MyMatrix[S, One](tMaxVec).t * MyMatrix.inverse(covT) * MyMatrix[S, One](tMaxVec)).mat(0, 0)
    val f = (y.length - covU.cols)/(covU.cols * (y.length - 1)) * t2
    1 - new FDistribution(covU.cols, y.length - covU.cols).cdf(f)
  }
}

case class QuantNoCovUniScoreTest(y: DenseVector[Double], x: DenseVector[Double]) extends ScoreTest {
  lazy val my: Double = mean(y)
  lazy val vy: Double = variance(y)
  lazy val n: Int = y.length
  lazy val u: Double = y.map(_ - my).t * x
  lazy val v: Double = vy * ( x.t * x - 1/n * pow(sum(x), 2))
}

case class QuantCovUniScoreTest(y: DenseVector[Double], x: DenseVector[Double], cov: DenseMatrix[Double]) extends ScoreTest {
  lazy val baseModel = new LinearRegression(y, cov)
  lazy val my: DenseVector[Double] = baseModel.estimates
  lazy val vy: Double = 1/n * pow(norm(y - my), 2)
  lazy val n: Int = y.length
  lazy val u: Double = (y - my).t * x
  lazy val v: Double = {
    val tmp1 = MyMatrix[One, K](x * baseModel.xs)
    val tmp2 = MyMatrix.inverse(MyMatrix[K, N](baseModel.xs.t) * MyMatrix[N, K](baseModel.xs))
    vy * (pow(norm(x), 2) - (tmp1 * tmp2 * tmp1.t).mat(0, 0))
  }
}

case class QuantCovMultiScoreTest(y: DenseVector[Double],
                                  x: DenseMatrix[Double],
                                  cov: DenseMatrix[Double]) extends ScoreTest {
  lazy val u = 0
  lazy val v = 0
  lazy val n = y.length
  lazy val k = baseModel.coefficients.length
  lazy val s = x.cols
  lazy val baseModel = new LinearRegression(y, cov)
  lazy val my: DenseVector[Double] = baseModel.estimates
  lazy val vy: Double = 1/n * pow(norm(y - my), 2)
  lazy val uVec: DenseVector[Double] = (MyMatrix[N, S](x).t * MyMatrix[N, One](y - my)).mat(::, 0)
  private lazy val tmp0 = MyMatrix[N, One](DenseVector.fill(n)(1.0)).t * MyMatrix[N, S](x :* x)
  private lazy val tmp1 = MyMatrix[N, S](x).t * MyMatrix[N, K](baseModel.xs)
  private lazy val tmp2 = MyMatrix.inverse(MyMatrix[K, N](baseModel.xs.t) * MyMatrix[N, K](baseModel.xs))
  lazy val vVec = vy * (tmp0.mat(0, ::).t - diag((tmp1 * tmp2 * tmp1.t).mat))
  lazy val tVec = uVec :/ pow(vVec, 0.5)
  lazy val tMax = max(tVec)
  override lazy val pValue = {
    val tmp = MyMatrix[N, N](diag(y - my)) * (MyMatrix[N, S](x) - MyMatrix[N, K](baseModel.xs) * tmp2.t * tmp1.t)
    val covU = (tmp.t * tmp).mat
    val dU = diag(pow(diag(covU), 0.5))
    val covT = MyMatrix[S, S](dU) * MyMatrix[S, S](covU) * MyMatrix[S, S](dU)
    val tMaxVec = DenseVector.fill(s)(this.tMax)
    val t2 = n * (MyMatrix[S, One](tMaxVec).t * MyMatrix.inverse(covT) * MyMatrix[S, One](tMaxVec)).mat(0, 0)
    val f = (n - s)/(s * (n - 1)) * t2
    1 - new FDistribution(s, n - s).cdf(f)
  }
}
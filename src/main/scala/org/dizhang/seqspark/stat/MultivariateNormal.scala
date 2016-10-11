package org.dizhang.seqspark.stat

import org.dizhang.seqspark.util.General._
import breeze.linalg.{*, diag, DenseMatrix => DM, DenseVector => DV}
import com.sun.jna.{Library, Native}
import com.sun.jna.ptr._

/**
  * multi-variate normal distribution cdf
  * call native fortran library built from Genz et al.
  *
  */
@SerialVersionUID(7778660001L)
trait MultivariateNormal extends Serializable {
  def numVariates: Int = mu.length
  def mu: DV[Double]
  def cov: DM[Double]

  def corr: DM[Double] = {
    val sd = diag(cov).map(x => 1.0/x.sqrt)

    val tmp = cov(::, *) :* sd

    tmp(*, ::) :* sd
  }

  def cdf(cutoff: DV[Double]): MultivariateNormal.CDF = {
    val corrMat = corr
    val corrArr =
      (for {
        i <- 1 until corr.rows
        j <- 0 until i
      } yield corrMat(i, j)).toArray
    val lower = Array.fill(numVariates)(0.0)
    val upper = (cutoff - mu) :/ diag(cov).map(x => x.sqrt)
    val infin = Array.fill(numVariates)(0)
    val maxpts = 10000 * numVariates
    val abseps = 1e-8
    val releps = 0.0
    MultivariateNormal.cdf(numVariates, lower, upper.toArray, infin, corrArr, maxpts, abseps, releps)
  }
}

object MultivariateNormal {

  @SerialVersionUID(7778660101L)
  case class CDF(pvalue: Double, error: Double, inform: Int) extends Serializable

  trait MVNImpl extends Library {
    def mvndst_(N: IntByReference,
               LOWER: Array[Double],
               UPPER: Array[Double],
               INFIN: Array[Int],
               CORREL: Array[Double],
               MAXPTS: IntByReference,
               ABSEPS: DoubleByReference,
               RELEPS: DoubleByReference,
               ERROR: DoubleByReference,
               VALUE: DoubleByReference,
               INFORM: IntByReference)
  }

  object MVNImpl {
    def Instance = Native.loadLibrary("mvn", classOf[MVNImpl]).asInstanceOf[MVNImpl]
  }

  def cdf(n: Int, lower: Array[Double], upper: Array[Double], infin: Array[Int],
          correl: Array[Double], maxpts: Int, abseps: Double, releps: Double): CDF = {
    val v: DoubleByReference = new DoubleByReference(0.0)
    val e: DoubleByReference = new DoubleByReference(0.0)
    val i: IntByReference = new IntByReference(0)
    MVNImpl.Instance.mvndst_(new IntByReference(n), lower, upper, infin, correl,
      new IntByReference(maxpts), new DoubleByReference(abseps),
      new DoubleByReference(releps), e, v, i)
    CDF(v.getValue, e.getValue, i.getValue)
  }

  @SerialVersionUID(7778660201L)
  final case class Centered(cov: DM[Double]) extends MultivariateNormal {
    def mu = DV.fill(cov.cols)(0.0)
  }

}

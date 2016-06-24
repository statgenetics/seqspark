package org.dizhang.seqspark.stat

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.{DiffFunction, LBFGS}

/**
  * linear and logistic regression
  */
@SerialVersionUID(7778730001L)
trait Regression extends Serializable {
  def responses: DenseVector[Double]
  def residuals = responses - estimates
  def ones = DenseVector.ones[Double](responses.length)
  lazy val xs = DenseMatrix.horzcat(ones.toDenseMatrix.t, independents)
  def independents: DenseMatrix[Double]
  def coefficients: DenseVector[Double]
  def estimates: DenseVector[Double]
  def information: DenseMatrix[Double]

}

object Regression {
  @SerialVersionUID(7778730101L)
  trait Result extends Serializable {
    def responses: DenseVector[Double]
    def estimates: DenseVector[Double]
    def xs: DenseMatrix[Double]
    def information: DenseMatrix[Double]
  }
}

/**
  * Linear regression mode for quantitative traits
  * Use the QR method to compute the coefficients
  */
@SerialVersionUID(7778540001L)
case class LinearRegression(responses: DenseVector[Double], independents: DenseMatrix[Double])
  extends Regression {

  val coefficients = {
    val qrMatrix = qr(xs)
    val r1 = qrMatrix.r(0 until xs.cols, ::)
    val q1 = qrMatrix.q(::, 0 until xs.cols)
    inv(r1) * (q1.t * responses)
  }

  val estimates = xs * coefficients

  lazy val residualsVariance = sum(pow(residuals, 2))/residuals.length.toDouble
  lazy val xsRotated = if (rank(xs) < xs.cols) svd(xs).leftVectors else xs
  lazy val information: DenseMatrix[Double] = xsRotated.t * xsRotated * residualsVariance

}

/**
  * Implement the logistic assoc with breeze
  * Use the LBFGS optimization method
  */
@SerialVersionUID(7778560001L)
case class LogisticRegression(responses: DenseVector[Double],
                              independents: DenseMatrix[Double])
  extends Regression {

  /**
    * z contains n elements (samples), is the result of xs * beta
    */
  def sigmoid(z: DenseVector[Double]): DenseVector[Double] =
    ones :/ (exp(-1.0 * z) + ones)

  /** beta contains p elements (variable numbers plus 1 intercept)
    * g [n]: probability of n samples
    * */
  val cost = new DiffFunction[Vector[Double]] {
    def calculate(beta: Vector[Double]) = {
      val num = ones.length
      val g = sigmoid(xs * beta)

      /** deviance: -2 * sum(y*log(g) + (1 - y)*log(1 - g)) */
      val value: Double = -2.0 * (responses.t * log(g) + (ones - responses).t * log(ones - g))

      /** gradient on beta: -2 * sum(x * (y - g)) */
      //val gradient: Vector[Double] = -2.0 * (xs.t * (response.:*(ones - g) - g.:*(ones - response)))
      val gradient: Vector[Double] = -2.0 * (xs.t * (responses - g))

      (value, gradient)
    }
  }

  val model = new LBFGS[Vector[Double]]

  /** the estimated betas */
  val coefficients = model.minimize(cost, DenseVector.fill[Double](independents.cols + 1)(0.01)).toDenseVector

  /** the estimated probabilities of responses equal to 1 */
  val estimates = sigmoid(xs * coefficients)

  lazy val residualsVariance: DenseVector[Double] = estimates :* estimates.map(1.0 - _)

  lazy val xsRotated = if (rank(xs) < xs.cols) svd(xs).leftVectors else xs

  lazy val information: DenseMatrix[Double] = {
    /** this is essentially (xs.t * diag(residualsVariance) * xs) */
    xsRotated.t * (xsRotated(::, *) :* residualsVariance)
  }

}

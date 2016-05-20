package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix, DenseVector, qr, rank, svd}

/**
  * linear and logistic regression
  */
@SerialVersionUID(5L)
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
  @SerialVersionUID(51L)
  trait Result extends Serializable {
    def responses: DenseVector[Double]
    def estimates: DenseVector[Double]
    def xs: DenseMatrix[Double]
    def information: DenseMatrix[Double]
  }
  case class LinearResult(xs: DenseMatrix[Double],
                          responses: DenseVector[Double],
                          estimates: DenseVector[Double],
                          residuals: DenseVector[Double],
                          residualsVariance: Double,
                          informationInverse: DenseMatrix[Double]) extends Result
  case class LogisticResult(xs: DenseMatrix[Double],
                            responses: DenseVector[Double],
                            estimates: DenseVector[Double],
                            residuals: DenseVector[Double],
                            residualsVariance: DenseVector[Double],
                            informationInverse: DenseMatrix[Double]) extends Result
}
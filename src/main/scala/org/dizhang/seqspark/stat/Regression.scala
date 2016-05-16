package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix, DenseVector}

/**
  * linear and logistic regression
  */
@SerialVersionUID(5L)
trait Regression extends Serializable {
  def responses: DenseVector[Double]
  def xs: DenseMatrix[Double]
  def independents: DenseMatrix[Double]
  def coefficients: DenseVector[Double]
  def estimates: DenseVector[Double]
  lazy val residuals: DenseVector[Double] = responses - estimates
  def information: DenseMatrix[Double]
  def informationInverse: DenseMatrix[Double]
}

object Regression {
  @SerialVersionUID(51L)
  trait Result extends Serializable
  case class LinearResult(xs: DenseMatrix[Double],
                          residuals: DenseVector[Double],
                          residualsVariance: Double,
                          informationInverse: DenseMatrix[Double]) extends Result
  case class LogisticResult(xsRV: DenseMatrix[Double],
                            residuals: DenseVector[Double],
                            informationInverse: DenseMatrix[Double]) extends Result
}
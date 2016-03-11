package org.dizhang.seqspark.stat

import breeze.linalg.{DenseVector, DenseMatrix}

/**
  * linear and logistic regression
  */
@SerialVersionUID(5L)
trait Regression extends Serializable {
  def xs: DenseMatrix[Double]
  def coefficients: DenseVector[Double]
  def estimates: DenseVector[Double]
}

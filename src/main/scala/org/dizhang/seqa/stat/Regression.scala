package org.dizhang.seqa.stat

import breeze.linalg.DenseVector

/**
  * linear and logistic regression
  */
@SerialVersionUID(5L)
trait Regression extends Serializable {
  def xs: DenseVector[Double]
  def coefficients: DenseVector[Double]
  def estimates: DenseVector[Double]
}

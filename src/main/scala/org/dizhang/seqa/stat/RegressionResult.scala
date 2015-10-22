package org.dizhang.seqa.stat

import breeze.linalg.{DenseVector, DenseMatrix}

/**
 * Hold regression result
 */
class RegressionResult(coefficients: DenseMatrix[Double]) {

  def estimates: DenseVector[Double] = coefficients(::, 0)

  def stdErrs: DenseVector[Double] = coefficients(::, 1)

  def statistics: DenseVector[Double] = coefficients(::, 2)

  def pValues: DenseVector[Double] = coefficients(::, 3)

}

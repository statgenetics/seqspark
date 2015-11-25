package org.dizhang.seqa.stat

import breeze.linalg._

/**
  * Linear regression mode for quantitative traits
  * Use the QR method to compute the coefficients
  */
//@SerialVersionUID(6L)
class LinearRegression(response: DenseVector[Double], independents: DenseMatrix[Double])
  extends Regression {

  lazy val ones = DenseVector.ones[Double](response.length)

  lazy val xs = DenseMatrix.horzcat(ones.toDenseMatrix.t, independents)

  lazy val coefficients = {
    val qrMatrix = qr(xs)
    val r1 = qrMatrix.r(0 until xs.cols, ::)
    val q1 = qrMatrix.q(::, 0 until xs.cols)
    inv(r1) * (q1.t * response)
  }

  lazy val estimates = independents * coefficients

}

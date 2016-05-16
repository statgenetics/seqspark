package org.dizhang.seqspark.stat

import breeze.linalg._
import breeze.numerics.pow
import org.dizhang.seqspark.util.General._

/**
  * Linear regression mode for quantitative traits
  * Use the QR method to compute the coefficients
  */
//@SerialVersionUID(6L)
class LinearRegression(val responses: DenseVector[Double], val independents: DenseMatrix[Double])
  extends Regression {

  val ones = DenseVector.ones[Double](responses.length)

  /** n x (k + 1) */
  val xs = DenseMatrix.horzcat(ones.toDenseMatrix.t, independents)

  val coefficients = {
    val qrMatrix = qr(xs)
    val r1 = qrMatrix.r(0 until xs.cols, ::)
    val q1 = qrMatrix.q(::, 0 until xs.cols)
    inv(r1) * (q1.t * responses)
  }

  val estimates = xs * coefficients

  lazy val residualsVariance = sum(pow(residuals, 2))/residuals.length.toDouble

  lazy val information: DenseMatrix[Double] = {
    xs.t * xs * residualsVariance
  }
  lazy val informationInverse = inv(information)
}

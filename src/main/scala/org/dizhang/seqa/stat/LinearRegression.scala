package org.dizhang.seqa.stat

import breeze.linalg._
import breeze.numerics.{abs, sqrt}
import breeze.stats.distributions.StudentsT
import org.dizhang.seqa.util.Opt

/**
 * Linear regression mode for quantitative traits
 */
@SerialVersionUID(6L)
class LinearRegression(y: DenseVector[Double], x: DenseVector[Double], cov: Opt[DenseMatrix[Double]] = None)
  extends Serializable {
  val ones = DenseVector.ones[Double](x.length)

  val xMat: DenseMatrix[Double] = cov.option match {
    case Some(c) => DenseMatrix.horzcat(DenseVector.horzcat(ones, x), c)
    case None => DenseVector.horzcat(ones, x)
  }
  

  var _summary: Option[RegressionResult] = None

  def summary: RegressionResult = {
    _summary match {
      case None =>
        val estimates: DenseVector[Double] = {
          val qrMats = qr(xMat)
          val r1 = qrMats.r(0 until xMat.cols, ::)
          val q1 = qrMats.q(::, 0 until xMat.cols)
          inv(r1) * (q1.t * y)
        }
        val dof = x.length - xMat.cols
        val sse = y.t * (DenseMatrix.eye (x.length) - xMat * estimates)
        val mse = sse / dof
        val covCoef = mse * inv (xMat.t * xMat)
        val sdBeta = sqrt (diag (covCoef) )
        val tStatic = estimates :/ sdBeta
        val tDist = new StudentsT (dof)
        val pValues = tStatic.map (t => 2.0 * tDist.cdf (abs (t) ) )
        val res = new RegressionResult(DenseVector.horzcat (estimates, sdBeta, tStatic, pValues) )
        _summary = Some(res)
        res
      case Some(r: RegressionResult) => r
    }
  }
}

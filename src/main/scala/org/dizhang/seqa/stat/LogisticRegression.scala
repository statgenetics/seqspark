package org.dizhang.seqa.stat

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.{DiffFunction, LBFGS}
import breeze.stats.distributions.Gaussian
import org.dizhang.seqa.util.Opt

object LogisticRegression {
  val y = breeze.linalg.DenseVector(new breeze.stats.distributions.Bernoulli(0.5).sample(100).map(if (_) 1.0 else 0.0).toArray)
  val x = breeze.linalg.DenseVector(breeze.stats.distributions.Gaussian(0.0, 1.0).sample(100).toArray)
}

/**
 * Implement the logistic assoc with breeze
 */
@SerialVersionUID(5L)
class LogisticRegression(y: DenseVector[Double], x: DenseVector[Double], cov: Opt[DenseMatrix[Double]] = None)
  extends Serializable {

  /**
   * y: response variable
   * x: genotype, e.g. 0, 1, 2
   * cov: covariates matrix, row: sample
   * */

  val ones = DenseVector.ones[Double](x.length)

  /**
   * the matrix [n x p] (n: Sample size, p: parameter number) of all independent variables, including the first column of ones.
   * cov is optional, the client can just use DenseMatrix, not Option[DenseMatrix]
   * */
  val xMat: DenseMatrix[Double] = cov.option match {
    case Some(c) => DenseMatrix.horzcat(DenseVector.horzcat(ones, x), c)
    case None => DenseVector.horzcat(ones, x)
  }

  //z contains n elements (samples), is the result of xMat * beta
  def sigmoid(z: DenseVector[Double]): DenseVector[Double] =
    ones :/ (exp(-1.0 * z) + 1.0)

  /** beta contains p elements (variable numbers plus 1 intercept)
    * g [n]: probability of n samples
    * */
  val cost = new DiffFunction[Vector[Double]] {
    def calculate(beta: Vector[Double]) = {
      val num = x.length
      val g = sigmoid(xMat * beta)

      /** deviance: -2 * sum(y*log(g) + (1 - y)*log(1 - g)) */
      val value: Double = -2.0 * (y.t * log(g) + (ones - y).t * log(ones - g))

      /** gradient on beta: -2 * sum(x * (y*(1 - g) - (1-y)g)) */
      val gradient: Vector[Double] = -2.0 * (xMat.t * (y.:*(ones - g) - g.:*(ones - y)))

      (value, gradient)
    }
  }

  val model = new LBFGS[Vector[Double]]


  //val deviance = cost.valueAt(estimates)

  private var _summary: Option[RegressionResult] = None

  def summary: RegressionResult = {
    _summary match {
      case None =>
        val estimates: DenseVector[Double] = cov.option match {
          case Some(c) => model.minimize(cost, DenseVector.fill[Double](c.cols + 2)(0.01)).toDenseVector
          case None => model.minimize(cost, DenseVector.fill[Double](2)(0.01)).toDenseVector
        }
        val p = sigmoid(xMat * estimates)
        val q = ones - p
        val information: DenseMatrix[Double] = xMat.t * (diag(p :* q) * xMat)
        val sdBeta = sqrt(diag(inv(information)))
        val wald = estimates :/ sdBeta
        val gaussian = new Gaussian(0.0, 1.0)
        val pValues = wald.map(z => 2.0 * gaussian.cdf(-abs(z)))
        val res = new RegressionResult(DenseVector.horzcat[Double](estimates, sdBeta, wald, pValues))
        _summary = Some(res)
        res
      case Some(r) => r
    }
  }
}

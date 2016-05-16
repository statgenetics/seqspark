package org.dizhang.seqspark.stat

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.{DiffFunction, LBFGS}

/**
  * Implement the logistic assoc with breeze
  * Use the LBFGS optimization method
  */

class LogisticRegression(val responses: DenseVector[Double],
                         val independents: DenseMatrix[Double])
  extends Regression {

  /**
   * response: response variable
   * xs: independents with ones
   * */
  val ones = DenseVector.ones[Double](independents.rows)

  /**
    * the matrix [n x p] (n: Sample size, p: parameter number) of all independent variables,
    * including the first column of ones.
    * */
  val xs = DenseMatrix.horzcat(ones.toDenseMatrix.t, independents)

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

  lazy val information: DenseMatrix[Double] = {
    /** this is essentially (xs.t * diag(residualsVariance) * xs) */
    xs.t * (xs(::, *) :* residualsVariance)
  }
  lazy val informationInverse = inv(information)

  lazy val xsRV = (xs(::, *) :* residualsVariance).t
}

/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.stat

import breeze.linalg._
import breeze.numerics._
import breeze.optimize.{DiffFunction, LBFGS, LBFGSB, GradientTester}

/**
  * linear and logistic regression
  */
@SerialVersionUID(7778730001L)
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
  @SerialVersionUID(7778730101L)
  trait Result extends Serializable {
    def responses: DenseVector[Double]
    def estimates: DenseVector[Double]
    def xs: DenseMatrix[Double]
    def information: DenseMatrix[Double]
  }
}

/**
  * Linear regression mode for quantitative traits
  * Use the QR method to compute the coefficients
  */
@SerialVersionUID(7778540001L)
case class LinearRegression(responses: DenseVector[Double], independents: DenseMatrix[Double])
  extends Regression {

  val coefficients = {
    val qrMatrix = qr.reduced(xs)
    val r1 = qrMatrix.r
    val q1 = qrMatrix.q
    inv(r1) * (q1.t * responses)
  }

  val estimates = xs * coefficients

  lazy val residualsVariance = sum(pow(residuals, 2))/(residuals.length - xs.cols).toDouble
  lazy val xsRotated = xs //if (rank(xs) < xs.cols) svd(xs).leftVectors else xs
  lazy val information: DenseMatrix[Double] = xsRotated.t * xsRotated / residualsVariance

}

/**
  * Implement the logistic assoc with breeze
  * Use the LBFGS optimization method
  */
@SerialVersionUID(7778560001L)
case class LogisticRegression(responses: DenseVector[Double],
                              independents: DenseMatrix[Double])
  extends Regression {

  /**
    * z contains n elements (samples), is the result of xs * beta
    */
  def sigmoid(z: DenseVector[Double]): DenseVector[Double] =
    ones /:/ (exp(-1.0 * z) + ones)

  /** beta contains p elements (variable numbers plus 1 intercept)
    * g [n]: probability of n samples
    * */
  val cost = new DiffFunction[DenseVector[Double]] {
    def calculate(beta: DenseVector[Double]) = {
      val num = ones.length
      val g = sigmoid(xs * beta)

      /** deviance: -(1/m) * sum(y*log(g) + (1 - y)*log(1 - g)) */
      val value: Double = -1.0/xs.rows * (responses.t * log(g) + (ones - responses).t * log(ones - g))

      /** gradient on beta: -(1/m) * sum(x * (y - g)) */
      val gradient: DenseVector[Double] = -1.0/xs.rows * (xs.t * (responses - g))

      (value, gradient)
    }
  }

  //val model3 = new TruncatedNewtonMinimizer[DenseVector[Double], ]()

  val model2 = new LBFGS[DenseVector[Double]](maxIter = 1000, tolerance = 1e-5)

  val model = new LBFGSB(DenseVector.fill(xs.cols)(-1.0), DenseVector.fill(xs.cols)(1.0))

  def test(p: DenseVector[Double]) = GradientTester.test[Int, DenseVector[Double]](cost, p)

  /** the estimated betas */
  val coefficients = model2.minimize(cost, DenseVector.fill[Double](independents.cols + 1)(0.001))

  /** the estimated probabilities of responses equal to 1 */
  val estimates = sigmoid(xs * coefficients)

  lazy val residualsVariance: DenseVector[Double] = estimates *:* estimates.map(1.0 - _)

  lazy val xsRotated = xs //if (rank(xs) < xs.cols) svd(xs).leftVectors else xs

  lazy val information: DenseMatrix[Double] = {
    /** this is essentially (xs.t * diag(residualsVariance) * xs) */
    xsRotated.t * (xsRotated(::, *) *:* residualsVariance)
  }
}

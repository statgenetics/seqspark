package org.dizhang.seqspark.stat

import breeze.linalg.DenseVector
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test for logistic regression implementation
  */
class LogisticRegressionSpec extends FlatSpec with Matchers {
  /**
  val seed = 10086
  implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
  val y = breeze.linalg.DenseVector(new breeze.stats.distributions.Bernoulli(0.5).sample(100).map(if (_) 1.0 else 0.0).toArray)
  val x = breeze.linalg.DenseVector(breeze.stats.distributions.Gaussian(0.0, 1.0).sample(100).toArray)
  */
  "A LogisticRegression" should "do nothing" in {
    //println("Do not do anything here")
    val rb = new Binomial(1, 0.5)
    val y = DenseVector(rb.sample(2000).map(_.toDouble):_*)
    val cov = DenseVector(rb.sample(2000).map(_.toDouble):_*).toDenseMatrix.t
    val model = LogisticRegression(y, cov)
    println(s"Logistic regression using LBFGS: ${model.coefficients}")
  }
}

package org.dizhang.seqspark.stat

import breeze.stats.distributions.{ThreadLocalRandomGenerator, RandBasis}
import org.apache.commons.math3.random.MersenneTwister
import org.scalatest.{Matchers, FlatSpec}

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
  }
}

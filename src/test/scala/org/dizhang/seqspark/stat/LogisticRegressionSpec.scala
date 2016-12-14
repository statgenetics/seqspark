package org.dizhang.seqspark.stat

import breeze.linalg._
import breeze.stats.distributions._
import breeze.stats._
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

  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
  }

  "A LogisticRegression" should "do nothing" in {
    //println("Do not do anything here")
    val rb = Binomial(1, 0.5)
    val sampleSize = 2000
    val y = DenseVector(rb.sample(sampleSize).map(_.toDouble):_*)
    val bmi = DenseVector(new Gaussian(25.0, 2.0).sample(sampleSize):_*)
    val age = DenseVector(new Gaussian(45, 5).sample(sampleSize):_*)
    val sex = DenseVector(rb.sample(sampleSize).map(_.toDouble):_*)
    val cov = DenseVector.horzcat((sex - mean(sex))/stddev(sex), (bmi - mean(bmi))/stddev(bmi), (age - mean(age))/stddev(age))

    //val cov = DenseVector.horzcat(sex)

    //println(s"y = c(${y.toArray.mkString(",")})")
    //println(s"x = c(${cov.toDenseVector.toArray.mkString(",")})")
    val model = LogisticRegression(y, cov)
    //println(s"coef: ${model.coefficients}")

    /**
    println(s"test: ${model.test(DenseVector.fill(cov.cols + 1)(0.99)).toString}")
    println(s"test: ${model.test(DenseVector.fill(cov.cols + 1)(0.5)).toString}")
    println(s"test: ${model.test(DenseVector.fill(cov.cols + 1)(0.25)).toString}")
    println(s"test: ${model.test(DenseVector.fill(cov.cols + 1)(-0.5)).toString}")
    println(s"test: ${model.test(DenseVector.fill(cov.cols + 1)(-0.25)).toString}")

    time {
      for (i <- 0 to 999) {
        LogisticRegression(y, cov)
        val lm = LinearRegression(y, cov)
      }
      //println(s"O Logistic regression using LBFGS: ${model.coefficients}")
      //println(s"O linear regression using LBFGS: ${lm.coefficients}")
    }("O")
    val newY = model.estimates.map(e => if (new Bernoulli(e).draw()) 1.0 else 0.0)
    time {
      val ny = model.estimates.map(e => if (new Bernoulli(e).draw()) 1.0 else 0.0)
      //println(ny)
    }("G")
    time {
      val model2 = LogisticRegression(newY, cov)
      val lm2 = LinearRegression(newY, cov)
      println(s"N Logistic regression using LBFGS: ${model2.coefficients}")
      println(s"N Linear regression using LBFGS: ${lm2.coefficients}")
    }("N")
    */
  }
}

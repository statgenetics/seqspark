package org.dizhang.seqspark.stat

import breeze.linalg._
import org.scalatest.{FlatSpec, Matchers}
import breeze.stats.distributions._

/**
  * Created by zhangdi on 12/1/16.
  */
class ScoreTestSpec extends FlatSpec with Matchers {
    val rg = new Gaussian(10.0, 3.0)
    val bg = Binomial(1, 0.6)
    val bg2 = Binomial(3, 0.005)
    val sampleSize = 2000

    def rbinom2(n: Int, m: Int, r: Binomial): DenseMatrix[Double] = {
      DenseVector.horzcat((1 to m).map(i => rbinom(n, r)): _*)
    }

    def rbinom(n: Int, r: Binomial): DenseVector[Double] = {
      DenseVector(r.sample(n).map(_.toDouble): _*)
    }
    def rnorm1(n: Int): DenseVector[Double] = {
      DenseVector(rg.sample(n): _*)
    }

    def rnorm2(n: Int, m: Int): DenseMatrix[Double] = {
      DenseVector.horzcat((1 to m).map(i => rnorm1(n)): _*)
    }

    val lr = LinearRegression(rnorm1(sampleSize), rnorm2(sampleSize, 5))
    val lgr = LogisticRegression(rbinom(sampleSize, bg), rnorm2(sampleSize, 5))
    val nm = ScoreTest.NullModel(lr)
    val nm2 = ScoreTest.NullModel(lgr)

  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
  }

    "A ScoreTest" should "be fine" in {
      val size = 500
      val xx = rbinom2(sampleSize, size, bg2)
      time {
        val kernel: DenseMatrix[Double] = {
          val rho = 0.3
          (1.0 - rho) * DenseMatrix.eye[Double](size) + DenseMatrix.fill[Double](size, size)(rho)
        }
        val x = xx * cholesky(kernel).t
        val st = ScoreTest(nm, x)
        println(s"${st.variance(0,0)}")
      }{"all"}

    }

}

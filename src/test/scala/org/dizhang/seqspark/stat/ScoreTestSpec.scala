package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix, DenseVector, diag}
import org.scalatest.{FlatSpec, Matchers}
import breeze.stats.distributions._

/**
  * Created by zhangdi on 12/1/16.
  */
class ScoreTestSpec extends FlatSpec with Matchers {
    val rg = new Gaussian(10.0, 3.0)
    val bg = new Binomial(1, 0.6)

    def rbinom(n: Int): DenseVector[Double] = {
      DenseVector(bg.sample(n).map(_.toDouble): _*)
    }
    def rnorm1(n: Int): DenseVector[Double] = {
      DenseVector(rg.sample(n): _*)
    }

    def rnorm2(n: Int, m: Int): DenseMatrix[Double] = {
      DenseVector.horzcat((1 to m).map(i => rnorm1(n)): _*)
    }

    val lr = LinearRegression(rnorm1(2000), rnorm2(2000, 5))
    val lgr = LogisticRegression(rbinom(2000), rnorm2(2000, 5))
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
      val x = rnorm2(2000, 500)

      time {
        val st = ScoreTest(nm2, x)
        println(s"${st.score(0)} ${st.variance(0,0)}")
      }{"all"}
      time {
        val res = for {
          i <- 0 to 499
          st = ScoreTest(nm2, x(::, i))
        } yield (st.score(0), st.variance(0,0))
        println(res(0))
      }("sep")

    }

}

package org.dizhang.seqspark.numerics

import breeze.linalg.DenseVector
import breeze.numerics.pow
import breeze.stats.distributions.ChiSquared
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhangdi on 12/13/16.
  */
class Qk21Spec extends FlatSpec with Matchers {

  val chisq = ChiSquared(1.0)

  def f(input: DenseVector[Double]): DenseVector[Double] = {
    input.map(x => chisq.pdf(x))
  }

  "A Qk21" should "behave well" in {
    val res = Qk21(f, 0.0, 1.0)
    println(res)
  }
}

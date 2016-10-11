package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Binomial
import org.scalatest.FlatSpec

/**
  * Created by zhangdi on 10/11/16.
  */
class MultivariateNormalSpec extends FlatSpec {
  val obj = {
    val rand = Binomial(2, 0.02)
    val dat = (0 to 10).map{i =>
      rand.sample(2000).map(_.toDouble)
    }
    val dm = DenseMatrix(dat: _*)
    val cov = dm * dm.t
    MultivariateNormal.Centered(cov)
  }
  "A MVN" should "be fine" in {
    val res = obj.cdf(DenseVector.fill(11)(0.0))
    println(s"pval: ${res.pvalue} err: ${res.error} info: ${res.inform}")
  }

}

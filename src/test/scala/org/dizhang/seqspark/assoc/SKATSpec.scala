package org.dizhang.seqspark.assoc

import breeze.linalg._
import breeze.stats.distributions.{Binomial, Gaussian, RandBasis, ThreadLocalRandomGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.random.MersenneTwister
import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.stat.{LinearRegression, ScoreTest}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.scalatest.FlatSpec

/**
  * Created by zhangdi on 11/23/16.
  */
class SKATSpec extends FlatSpec {
  val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(100)))

  val encode: Encode[Byte] = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.skat")
    val method = MethodConfig(conf)
    val randg = Binomial(3, 0.005)(randBasis)
    val gt = Array("0/0", "0/1", "1/1", "./.")
    val vars = (0 to 10).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(2000).map(g => Genotype.Raw.toSimpleGenotype(gt(g)))
      Variant.fromIndexedSeq(meta, geno, 16.toByte)
    }
    val sm = method.config.root().render()
    Encode(vars, None, None, None, sm)
  }

  val nullModel = {
    val rand = Gaussian(2, 0.25)(randBasis)
    val dat = (0 to 3).map{i =>
      rand.sample(2000)
    }
    val y = DenseVector(rand.sample(2000): _*)
    val dm = DenseMatrix(dat: _*)
    val reg = LinearRegression(y, dm.t)
    ScoreTest.NullModel(reg)
  }

  "A SKAT" should "be fine" in {
    for (r <- 0 to 10) {
      val sk = SKAT(nullModel, encode.getCoding, "liu.mod", r/10.0)
      val pv = sk.result.pValue.map(_.toString).getOrElse("NA")
      val l = sk.lambda.get.toArray.mkString(",")
      //println(s"r: ${r/10.0} pvalue: ${pv} q: ${sk.qScore} method: ${sk.x.config.misc.getString("method")} lambda: ${l} weight: ${sk.weight}")
    }
  }
}

package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
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
class SKATOSpec extends FlatSpec {
  val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(100)))

  val encode: Encode[Byte] = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.skato")
    val method = MethodConfig(conf)
    val randg = Binomial(3, 0.005)(randBasis)
    val gt = Array("0/0", "0/1", "1/1", "./.")
    val vars = (0 to 10).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(2000).map(g => Genotype.Raw.toSimpleGenotype(gt(g)))
      Variant.fromIndexedSeq(meta, geno, 16.toByte)
    }
    Encode(vars, None, None, None, method)
  }
  val nullModel: SKATO.NullModel = {
    val rand = Gaussian(2, 0.25)(randBasis)
    val dat = (0 to 3).map{i =>
      rand.sample(2000)
    }
    val y = DenseVector(rand.sample(2000): _*)
    val dm = DenseMatrix(dat: _*)
    val reg = LinearRegression(y, dm.t)
    SKATO.NullModel(reg)
  }
  "A SKATO" should "be fine" in {
    val so = SKATO(nullModel, encode)

    println(s"geno: ${so.geno.rows} x ${so.geno.cols} weight: ${so.weight.length}")
    //val sores = so.result
    //println(s"S: ${sores.statistic} P: ${sores.pValue.map(_.toString).getOrElse("NA")}")
  }
}

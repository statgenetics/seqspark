package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.{Binomial, Gaussian}
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.stat.{LinearRegression, ScoreTest}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
/**
  * Created by zhangdi on 10/11/16.
  */
class VTSpec extends FlatSpec {
  val encode = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.vt")
    val method = MethodConfig(conf)
    val randg = Binomial(2, 0.02)
    val vars = (0 to 10).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(2000).map(_.toByte)
      Variant.fromIndexedSeq(meta, geno, 0.toByte)
    }
    Encode(vars, None, None, None, method)
  }
  val nullModel = {
    val rand = Gaussian(2, 0.25)
    val dat = (0 to 3).map{i =>
      rand.sample(2000)
    }
    val y = DenseVector(rand.sample(2000): _*)
    val dm = DenseMatrix(dat: _*)
    val reg = LinearRegression(y, dm.t)
    ScoreTest.NullModel(reg)
  }
  "A VT" should "be fine" in {
    val vt = VT.AnalyticTest(nullModel, encode)
    println(s"S: ${vt.statistic} P: ${vt.pValue}")
  }
}

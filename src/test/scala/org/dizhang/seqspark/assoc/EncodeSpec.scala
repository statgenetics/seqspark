package org.dizhang.seqspark.assoc

import breeze.stats.distributions.Binomial
import com.typesafe.config.ConfigFactory
import org.dizhang.seqspark.assoc.Encode.SharedMethod
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.scalatest.FlatSpec

/**
  * Created by zhangdi on 10/11/16.
  */
class EncodeSpec extends FlatSpec {

  val encode = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.vt")
    val method = MethodConfig(conf)
    val randg = Binomial(2, 0.02)
    val vars = (0 to 10).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(1000).map(_.toByte)
      Variant.fromIndexedSeq(meta, geno, 0.toByte)
    }
    val sm = method.config.root().render()
    Encode(vars, None, None, None, sm)
  }

  "thresholds" should "work" in {
    val th = encode.thresholds.get
    //println(th.mkString(","))
  }

}

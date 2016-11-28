package org.dizhang.seqspark.assoc

import breeze.stats.distributions.Binomial
import com.typesafe.config.ConfigFactory
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark._
/**
  * Created by zhangdi on 10/11/16.
  */

object EncodeSpec {
  val vars = {
    val randg = Binomial(2, 0.02)
    (0 to 10).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(1000).map(_.toByte)
      Variant.fromIndexedSeq(meta, geno, 0.toByte)
    }
  }
}

class EncodeSpec extends FlatSpec with BeforeAndAfter with Matchers {

  private val master = "local[2]"
  private val appName = "test-spark"
  private var sc: SparkContext = _


  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }



  //"thresholds" should "work" in {
    //val a = sc.parallelize(Array.fill(10)(1.0))
    //val sm = ConfigFactory.load().getConfig("seqspark.association.method.vt").root().render()
    //val ec = a.map(x => Encode(EncodeSpec.vars, None, None, None, sm))
    //val cnt = ec.map(_.config.`type`).collect()
    //cnt.length should be (10)
    //val th = encode.thresholds.get
    //println(th.mkString(","))
  //}

}

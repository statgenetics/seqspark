package org.dizhang.seqspark.assoc

import breeze.linalg._
import breeze.stats.distributions.{Binomial, Gaussian, RandBasis, ThreadLocalRandomGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.random.MersenneTwister
import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.stat.{LCCSLiu, LinearRegression, ScoreTest}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.scalatest.FlatSpec

/**
  * Created by zhangdi on 11/23/16.
  */
class SKATSpec extends FlatSpec {
  val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(100)))

  def encode(n: Int): Encode[Byte] = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.skat")
    val method = MethodConfig(conf)
    val randg = Binomial(3, 0.005)(randBasis)
    val gt = Array("0/0", "0/1", "1/1", "./.")
    val vars = (0 until n).map{i =>
      val meta = Array("1", i.toString, ".", "A", "C", ".", ".", ".")
      val geno = randg.sample(2000).map(g => Genotype.Raw.toSimpleGenotype(gt(g)))
      Variant.fromIndexedSeq(meta, geno, 16.toByte)
    }
    val sm = method.config.root().render()
    Encode(vars, None, None, None, sm)
  }

  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
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
    //SKAT(nullModel, encode(10).getCoding, "davies", 0.0).pValue

    for (i <- List(10, 20, 50, 100, 200, 300, 500, 1000)) {
      //val cd = encode(i).getCoding
      //val sm = cd.asInstanceOf[Encode.Rare].coding
      for (j <- 0 to 0) {
        /**
        time {
          val sk = SKAT(nullModel, cd, "davies", 0.0)
          //val cs = SKAT.getMoments(sk.vc)
          //val lm = LCCSLiu.ModifiedMoments(cs)
          //println(s"cs: ${lm.cs.mkString(",")}\ns1: ${lm.s1} s2: ${lm.s2}")
          println(sk.pValue)
        }(s"SKAT Davies for $i variants: $j")

        time {
          val sk = SKAT(nullModel, cd, "liu.mod", 0.0)
          println(sk.pValue2)
          //val lb = SKAT.getLambda(sk.vc)
          //val lm = LCCSLiu.Modified(lb.get)
          //println(s"cs: ${lm.c1},${lm.c2},${lm.c3},${lm.c4} s1: ${lm.s1} s2: ${lm.s2}")
        } (s"SKAT LiuMod for $i variants: $j")

        time {
          val size = sm.cols
          val kernel: DenseMatrix[Double] = {
            val rho = 0.3
            (1.0 - rho) * DenseMatrix.eye[Double](size) + DenseMatrix.fill[Double](size, size)(rho)
          }
          val x = sm * cholesky(kernel).t
          val st = ScoreTest(nullModel, x)
          println(s"${st.variance(0,0)}")

        }(s"Munual SKAT for $i variants: $j")
          */
        //time {println(SKAT(nullModel, sm, "liu.mod", 0.3).vc(0,0))}(s"SKAT Matrix for $i variants: $j")

        //time {println(SKAT(nullModel, cd, "davies", 0.3).pValue)}(s"SKAT Davies for $i variants: $j")
      }
    }
  }
}

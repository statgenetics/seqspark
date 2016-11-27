package org.dizhang.seqspark.assoc

import java.io.{File, PrintWriter}

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.pow
import breeze.stats.distributions.{Binomial, Gaussian, RandBasis, ThreadLocalRandomGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.random.MersenneTwister
import org.dizhang.seqspark.assoc.Encode.SharedMethod
import org.dizhang.seqspark.assoc.SKATO.LiuPValue
import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.stat.{LinearRegression, ScoreTest}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.dizhang.seqspark.util.General._
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
    val sm = method.config.root().render()
    Encode(vars, None, None, None, sm)
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

    /**
    val pw = new PrintWriter(new File("skat.data"))
    val geno = so.geno.toDense
    for (i <- 0 until so.geno.cols) {
      pw.write(geno(::, i).toArray.mkString("\t") + "\n")
    }
    val y = so.nullModel.responses
    val cov = so.nullModel.xs
    pw.write(y.toArray.mkString("\t") + "\n")
    for (i <- 0 until cov.cols) {
      pw.write(cov(::, i).toArray.mkString("\t") + "\n")
    }
    pw.close()
      */
    //println("rhos: " + so.rhos.mkString(","))
    //println("qvals: " + so.qScores.mkString(","))
    //println("pvals: " + so.pValues.mkString(","))
    //println(so.param)
    //println("pmin.q" + so.pMinQuantiles.mkString(","))

    //val so = SKATO(nullModel, encode)
    /**
    so match {
      case x: LiuPValue =>
        for (i <- 1 to 10) {
          println(s"int ${pow(10.0, -i)}: ${x.integralFunc(pow(10.0, -i))}")
        }
        //println(s"adaptive quatrature ${quadrature(x.integralFunc, 0, 40)}")
        //println(s"simpson ${simpson(x.integralFunc, 1e-6,1, 2000)}")
        //println(s"tra ${trapezoid(x.integralFunc, 1e-6,1, 2000)}")
    }
    */
    //val pz = so.P0SqrtZ(0 to 5, ::)
    //for (i <- 0 until pz.rows) {
    //  println(pz(i, ::).t.toArray.mkString(","))
    //}
    //val sores = so.result
    //println(s"S: ${sores.statistic} P: ${sores.pValue.map(_.toString).getOrElse("NA")}")
  }
}

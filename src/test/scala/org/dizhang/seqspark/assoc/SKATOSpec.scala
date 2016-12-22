package org.dizhang.seqspark.assoc

import java.io.{File, PrintWriter}

import breeze.linalg.{DenseMatrix, DenseVector, linspace}
import breeze.numerics.pow
import breeze.stats.distributions.{Binomial, Gaussian, RandBasis, ThreadLocalRandomGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.random.MersenneTwister
import org.dizhang.seqspark.assoc.SKATO.LiuPValue
import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.stat.{LinearRegression, ScoreTest}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.dizhang.seqspark.util.General._
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 11/23/16.
  */
class SKATOSpec extends FlatSpec with Matchers {
  val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(100)))
  val logger = LoggerFactory.getLogger(getClass)

  def encode(num: Int): Encode[Byte] = {
    val conf = ConfigFactory.load().getConfig("seqspark.association.method.skato")
    val method = MethodConfig(conf)
    val randg = Binomial(3, 0.005)(randBasis)
    val gt = Array("0/0", "0/1", "1/1", "./.")
    val vars = (0 until num).map{i =>
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
  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
  }

  "A SKATO" should "be fine" in {



    for (i <- List(2, 5, 10, 100, 200, 300, 500, 1000)) {
      val cd = encode(i).getCoding
      for (j <- 0 to 3) {
        time {
          val so = SKATO(nullModel, cd, "liu.mod")
          val res = so.result
          logger.info(s"${res.toString}")
        }(s"$i variants ${j}th test liu")
        time {
          val so = SKATO(nullModel, cd, "davies")
          val res = so.result
          logger.info(s"${res.toString}")
        }(s"$i variants ${j}th test davies")
      }
    }

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
    /**
    val so = SKATO(nullModel, encode, "optimal.adj")
    //val xv = linspace(1e-10, 19 + 1e-10, 20)
    so match {
      case x: LiuPValue =>
        time {
          //for (i <- 0 to 10)
          //  println(x.pValue.get)
        }("adaptive")
        time {
          //for (i <- 0 to 10)
          //  println(x.pValue3.get)
        }("naive")

        //val old = for (i <- xv) yield x.integralFunc(i)
        //println(s"old: df=${x.df} " + old.toArray.mkString(","))
        //println(s"new: df=${x.df} " + x.integralFunc2(xv).toArray.mkString(","))

        //x.pValue.get should be (x.pValue3.get +- 5e-3)
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

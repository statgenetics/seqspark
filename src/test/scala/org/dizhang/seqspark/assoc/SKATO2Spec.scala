/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.assoc

import java.io.{File, PrintWriter}

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, SparseVector, linspace}
import breeze.numerics.pow
import breeze.stats.distributions.{Binomial, Gaussian, RandBasis, ThreadLocalRandomGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.commons.math3.random.MersenneTwister
import org.dizhang.seqspark.assoc.SKATO2.LiuPValue
import org.dizhang.seqspark.ds.{Genotype, Variant, Variation}
import org.dizhang.seqspark.stat.{LinearRegression}
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.dizhang.seqspark.util.General._
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 11/23/16.
  */
class SKATO2Spec extends FlatSpec with Matchers {
  val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(100)))
  val logger = LoggerFactory.getLogger(getClass)

  val nm: NM = {
    val file = scala.io.Source.fromURL(getClass.getResource("/pheno.tsv")).getLines().toArray
    val header = file.head.split("\t")
    val dat = for (s <- file.slice(1, file.length)) yield s.split("\t")
    val pheno = header.zip(for (i <- header.indices)
      yield DenseVector(dat.map(x => try {x(i).toDouble} catch {case e: Exception => 0.0}))).toMap
    NM(pheno("bmi"), DenseVector.horzcat(pheno("age"), pheno("sex"), pheno("disease")), true, false)
  }

  def geno(fn: String): CSCMatrix[Double] = {
    val file = scala.io.Source.fromURL(getClass.getResource(fn)).getLines().toArray
    val dat = for (s <- file) yield s.split(",")
    val idxes = for (i <- dat.head.indices) yield dat.map(_(i).toDouble).zipWithIndex.filter(_._1 != 0.0).map(_._2)
    val arrs = for (i <- dat.head.indices) yield dat.map(_(i).toDouble).filter(_ != 0.0)
    val res = SparseVector.horzcat(idxes.zip(arrs).map{p =>
      new SparseVector[Double](p._1, p._2, dat.length)
    }: _*)
    logger.info(s"${idxes.indices}")
    res
  }

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
  val nullModel: NM = {
    val rand = Gaussian(2, 0.25)(randBasis)
    val dat = (0 to 3).map{i =>
      rand.sample(2000)
    }
    val y = DenseVector(rand.sample(2000): _*)
    val dm = DenseMatrix(dat: _*)
    NM(y, dm.t, true, false)
  }
  def time[R](block: => R)(tag: String): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info(s"$tag Elapsed time: " + (t1 - t0)/1e6 + "ms")
    result
  }


  "A SKATO2" should "be fine" in {

    //val cda = geno("/geno2.dat")
    //val d = SKATO2(nm, Encode.Rare(cda, Array[Variation]()), "optimal")
    //val l = SKATO(nm, Encode.Rare(cda, Array[Variation]()), "liu.mod")


    //logger.info(d.result.toString)
    //logger.info("qscores: " + d.qScores.mkString(","))
    //logger.info(l.result.toString)

    for (i <- List(5, 10, 100)) {
      val cd = encode(i).getCoding.asInstanceOf[Encode.Rare]
      for (j <- 0 to 0) {
        time {
          val so = SKATO2(nullModel, cd, "liu.mod", Array[Double]())
          val res = so.result
          logger.info(s"${res.toString}")
        }(s"$i variants ${j}th test liu")
        time {
          val so = SKATO2(nullModel, cd, "davies", Array[Double]())
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

package org.dizhang.seqspark.stat

import java.util.logging

import breeze.linalg.DenseVector
import org.scalatest._
import org.dizhang.seqspark.stat.{LCCSDavies}

/**
  * Test the linear combination chi-square distribution class
  */

class LCCSDaviesSpec extends FlatSpec with Matchers {
  val flogger = logging.Logger.getLogger("com.github.fommil")
  flogger.setLevel(logging.Level.WARNING)

  trait SLCCS5 {
    val slccs = LCCSDavies.Simple(DenseVector.fill(5)(1.0))
    implicit val counter = () => 1
    implicit val toggler = () => false
  }
  trait SLCCS {
    val lb: Array[Double]
    def slccs = LCCSDavies.Simple(DenseVector(lb))
    def cdf(c: Double) = slccs.cdf(c)
  }
  "LinearCombChiSquare functions" should "work fine" in new SLCCS5 {
    val x = -49.9
    val y = -50.1
    assert(LCCSDavies.exp1(x) != 0.0, "e^-49.9 is not 0.0")
    assert(LCCSDavies.exp1(y) == 0.0, "e^-50.1 is euqal to 0.0")
    LCCSDavies.log1(0.021662, first = true) should be (0.021430 +- 1e-6)
    val res1 = slccs.errbd(0.369996)
    res1._1 should be (0.0235764 +- 1e-6)
    res1._2 should be (19.2302 +- 1e-4)
    val res2 = slccs.errbd(0.425285)
    res2._1 should be (7.65222e-5 +- 1e-8)
    res2._2 should be (33.4605 +- 1e-4)
    val res3 = slccs.ctff(5e-7, 1.42302)
    res3._1 should be (47.6907 +- 1e-4)
    res3._2 should be (4.26907 +- 1e-4)
    val res4 = slccs.ctff(5e-7, -1.42302)
    res4._1 should be (0.00342894 +- 1e-8)
    res4._2 should be (-728.589 +- 1e-2)
    slccs.truncation(1.264911, 0.0) should be (0.012508 +- 5e-7)
    slccs.truncation(5.059644, 0.0) should be (0.000391 +- 5e-7)
    slccs.truncation(20.238577, 0.0) should be (0.000012 +- 5e-7)
    slccs.truncation(80.954308, 0.0) should be (0.0 +- 5e-7)
    slccs.truncation(40.477154, 0.0) should be (0.000002 +- 5e-7)
    slccs.truncation(57.824506, 0.0) should be (0.000001 +- 5e-7)
    slccs.truncation(67.461923, 0.0) should be (0.000001 +- 5e-7)
    slccs.truncation(73.594826, 0.0) should be (0.0 +- 5e-7)
    slccs.truncation(73.594826, 0.000008) should be (0.0 +- 5e-7)
    slccs.findu(5.059644, 0.0000005) should be (73.594826 +- 5e-5)
    val res5 = slccs.integrate(5.0, 0.0, 0.0, 500, 0.1471790854, 0.0, mainx = true)
    res5._1 should be (-0.0841198140 +- 1e-6)
    res5._2 should be (1.6982059528 +- 1e-6)
  }
  "Method cfe" should "work fine" in new SLCCS5 {
    slccs.cfe(5.0) should be (0.030283 +- 1e-6)
  }
  "Method cdf" should "work fine" in new SLCCS5 {
    val res = slccs.cdf(5.0)
    println(res.toString)
    res.pvalue should be (0.58412 +- 1e6)
  }
  /**
  *it should "be ok" in new SLCCS {
    *override val lb: Array[Double] = Array(0.0, 1.0, 2.0, 3.0, 4.0)
    *val res1 = cdf(5.0)
    *print(res1.toString)
    *print(cdf(1.0))
    *print(cdf(2.0))
    *print(cdf(3.0))
    *print(cdf(4.0))
    *print(cdf(10.0))
  *}
    */
}















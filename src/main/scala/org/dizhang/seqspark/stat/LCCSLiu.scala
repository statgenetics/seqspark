package org.dizhang.seqspark.stat


import breeze.linalg.{DenseVector => DV, sum}
import breeze.numerics.pow
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.util.General.RichDouble
import org.dizhang.seqspark.stat.{LinearCombinationChiSquare => LCCS}
import LCCSLiu._

/**
  * Use Liu et al. to compute p value for
  * the linear combination of chi-square distributions.
  */
object LCCSLiu {

  case class CDFLiu(pvalue: Double) extends LCCS.CDF {
    def ifault = 0
    def trace = Array(0.0)
    override def toString = "Pvalue:   %10f".format(pvalue)
  }

  trait CentralOneDF extends LCCSLiu {
    def degreeOfFreedom = DV.ones[Double](size)
    def nonCentrality = DV.zeros[Double](size)
    override def ck(k: Int): Double = {
      sum(pow(lambda, k))
    }
  }

  trait Old extends LCCSLiu {
    val a = if (squareOfS1LargerThanS2) 1.0/(s1 - (s1.square - s2).sqrt) else 1.0/s1
    val df = if (squareOfS1LargerThanS2) a.square - 2 * delta else c2.cube/c3.square
  }
  trait New extends LCCSLiu {
    val a = if (squareOfS1LargerThanS2) 1.0/(s1 - (s1.square - s2).sqrt) else 1.0/s2.sqrt
    val df = if (squareOfS1LargerThanS2) a.square - 2 * delta else 1.0/s2
  }
  @SerialVersionUID(7778550101L)
  case class Simple(lambda: DV[Double]) extends LCCSLiu with CentralOneDF with Old
  @SerialVersionUID(7778550201L)
  case class Modified(lambda: DV[Double]) extends LCCSLiu with CentralOneDF with New
}
@SerialVersionUID(7778550001L)
trait LCCSLiu extends LinearCombinationChiSquare {

  def ck(k: Int): Double = {
    val lbk = pow(lambda, k)
    (lbk dot degreeOfFreedom) + k * (lbk dot nonCentrality)
  }
  val c1 = ck(1)
  val c2 = ck(2)
  val c3 = ck(3)
  val c4 = ck(4)
  val s1 = c3/c2.cube.sqrt
  val s2 = c4/c2.square
  val muQ = c1
  val sigmaQ = (2 * c2).sqrt
  private val squareOfS1LargerThanS2: Boolean = {
    s1.square > s2
  }
  val a: Double
  val delta = if (squareOfS1LargerThanS2) s1 * a.cube - a.square else 0.0
  val df: Double
  val sigmaX = 2.0.sqrt * a
  val muX = df + delta

  def cdf(cutoff: Double): CDFLiu = {
    val nccs = NonCentralChiSquare(df + delta, delta)
    val norm =  (cutoff - muQ)/sigmaQ
    val norm1 = norm * sigmaX + muX
    CDFLiu(nccs.cdf(norm1))
  }
}

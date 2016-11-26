package org.dizhang.seqspark.stat


import breeze.linalg.{sum, DenseVector => DV}
import breeze.numerics.pow
import org.dizhang.seqspark.stat.LCCSLiu._
import org.dizhang.seqspark.stat.{LinearCombinationChiSquare => LCCS}
import org.dizhang.seqspark.util.General.RichDouble

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

  trait CentralOneDF extends LinearCombinationChiSquare {
    def degreeOfFreedom = DV.ones[Double](size)
    def nonCentrality = DV.zeros[Double](size)
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
  def c1:Double = ck(1)
  def c2:Double = ck(2)
  def c3:Double = ck(3)
  def c4:Double = ck(4)
  def s1:Double = c3/c2.cube.sqrt
  def s2:Double = c4/c2.square
  def muQ:Double = c1
  def sigmaQ:Double = (2 * c2).sqrt
  protected lazy val squareOfS1LargerThanS2: Boolean = {
    s1.square > s2
  }
  def a: Double
  def delta:Double = if (squareOfS1LargerThanS2) s1 * a.cube - a.square else 0.0
  def df: Double
  def sigmaX:Double = 2.0.sqrt * a
  def muX:Double = df + delta

  def cdf(cutoff: Double): CDFLiu = {
    val nccs = NonCentralChiSquare(df + delta, delta)
    val norm =  (cutoff - muQ)/sigmaQ
    val norm1 = norm * sigmaX + muX
    CDFLiu(nccs.cdf(norm1))
  }
}

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

package org.dizhang.seqspark.stat


import breeze.linalg.{sum, DenseVector => DV}
import breeze.numerics.pow
import org.dizhang.seqspark.stat.LCCSLiu._
import org.dizhang.seqspark.stat.{LinearCombinationChiSquare => LCCS}
import org.dizhang.seqspark.util.General.RichDouble
import org.slf4j.LoggerFactory

/**
  * Use Liu et al. to compute p value for
  * the linear combination of chi-square distributions.
  */
object LCCSLiu {

  val logger = LoggerFactory.getLogger(getClass)

  case class CDFLiu(pvalue: Double, ifault: Int) extends LCCS.CDF {
    def trace = Array(0.0)
    override def toString = "Pvalue:   %10f".format(pvalue)
  }

  trait CentralOneDF extends LinearCombinationChiSquare {
    def degreeOfFreedom = DV.ones[Double](size)
    def nonCentrality = DV.zeros[Double](size)
  }

  trait Old extends LCCSLiu {
    def a = if (squareOfS1LargerThanS2) 1.0/(s1 - (s1.square - s2).sqrt) else 1.0/s1
    def df = if (squareOfS1LargerThanS2) a.square - 2 * delta else c2.cube/c3.square
  }
  trait New extends LCCSLiu {
    def a = if (squareOfS1LargerThanS2) 1.0/(s1 - (s1.square - s2).sqrt) else 1.0/s2.sqrt
    def df = if (squareOfS1LargerThanS2) a.square - 2 * delta else 1.0/s2
  }
  @SerialVersionUID(7778550101L)
  case class Simple(lambda: DV[Double]) extends LCCSLiu with CentralOneDF with Old {
    val c1 = ck(1)
    val c2 = ck(2)
    val c3 = ck(3)
    val c4 = ck(4)
  }
  @SerialVersionUID(7778550201L)
  case class Modified(lambda: DV[Double]) extends LCCSLiu with CentralOneDF with New {
    val c1 = ck(1)
    val c2 = ck(2)
    val c3 = ck(3)
    val c4 = ck(4)
  }
  case class SimpleMoments(cs: IndexedSeq[Double]) extends LCCSLiu with CentralOneDF with Old {
    def lambda = DV.zeros[Double](0)
    override val c1 = cs(0)
    override val c2 = cs(1)
    override val c3 = cs(2)
    override val c4 = cs(3)
  }
  case class ModifiedMoments(cs: IndexedSeq[Double]) extends LCCSLiu with CentralOneDF with New {
    def lambda = DV.zeros[Double](0)
    override val c1 = cs(0)
    override val c2 = cs(1)
    override val c3 = cs(2)
    override val c4 = cs(3)
  }
}
@SerialVersionUID(7778550001L)
trait LCCSLiu extends LinearCombinationChiSquare {

  def ck(k: Int): Double = {
    val lbk = pow(lambda, k)
    (lbk dot degreeOfFreedom) + k * (lbk dot nonCentrality)
  }
  def c1:Double
  def c2:Double
  def c3:Double
  def c4:Double
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
    //logger.debug(s"muX: $muX sigmaX: $sigmaX muQ: $muQ sigmaQ: $sigmaQ df: $df delta: $delta ")
    val nccs = NonCentralChiSquare(df + delta, delta)
    val norm =  (cutoff - muQ)/sigmaQ
    val norm1 = norm * sigmaX + muX
    val pv = nccs.cdf(norm1)
    if (pv >= 0.0 && pv <= 1.0) {
      CDFLiu(pv, 0)
    } else {
      CDFLiu(pv, 1)
    }
  }
}

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

import breeze.linalg.{inv, DenseMatrix => DM, DenseVector => DV}
import org.dizhang.seqspark.assoc.RareMetalWorker._
import org.dizhang.seqspark.ds.Counter.{CounterElementSemiGroup => cesg}
import org.dizhang.seqspark.ds.{Region, Variation}
import org.dizhang.seqspark.stat.ScoreTest
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.util.Constant

import scala.language.existentials
/**
  * raremetal worker
  * generate the summary statistics
  *
  * Because we don't want to compute the covariance involving
  * common variants, which are usually ignoreed in rare variant
  * association. We separate common and rare variants
  *
  * For computational efficiency, never use diag() for large densevector
  * breeze will try to make a densematrix, it is not necessary
  *
  */
@SerialVersionUID(7727790001L)
sealed trait RareMetalWorker extends AssocMethod {
  def nullModel: NM.Fitted
  def sampleSize: Int = nullModel.y.length
  def x: Encode.Coding
  def common: Option[Encode.Common] = x match {
    case c: Encode.Common => Some(c)
    case m: Encode.Mixed => Some(m.common)
    case _ => None
  }
  def rare: Option[Encode.Rare] = x match {
    case r: Encode.Rare => Some(r)
    case m: Encode.Mixed => Some(m.rare)
    case _ => None
  }
  def model: ScoreTest
  def score: DV[Double]
  def variance: DM[Double]
  def result: RareMetalWorker.RMWResult = {
      RareMetalWorker.DefaultRMWResult(binary = nullModel.binary, Array(sampleSize),
        commonAndRare(common.map(_.vars), rare.map(_.vars)), score, variance)
  }
}

object RareMetalWorker {

  val IK = Constant.Variant.InfoKey
  val SegmentSize = 1e6.toInt

  @SerialVersionUID(7727790101L)
  final case class Analytic(nullModel: NM.Fitted,
                            x: Encode.Coding) extends RareMetalWorker {
    val model = x match {
      case Encode.Common(c, v) => ScoreTest(nullModel, c)
      case Encode.Rare(r, v) => ScoreTest(nullModel, r)
      case Encode.Mixed(r, c) => ScoreTest(nullModel, c.coding, r.coding)
    }
    val score = model.score
    val variance = model.variance
  }

  @SerialVersionUID(7727790201L)
  trait RMWResult extends AssocMethod.Result {
    def segment: Region = {
      val some = vars.head
      val start = some.start/SegmentSize * SegmentSize
      Region(some.chr, start, start + SegmentSize)
    }
    def segmentId: Int = {
      segment.chr.toInt * SegmentSize + segment.start/SegmentSize
    }
    def binary: Boolean
    def sampleSizes: Array[Int]
    def vars: Array[Variation]
    def score: DV[Double]
    def variance: DM[Double]
    def ++(that: RMWResult): RMWResult = {
      val vm1 = this.vars.zipWithIndex.toMap
      val vm2 = that.vars.zipWithIndex.toMap
      val allVars = mergeVars(this.vars, that.vars)
      val sizes = this.sampleSizes ++ that.sampleSizes
      val index1 = allVars.map(k => if (vm1.contains(k)) vm1(k) else -1)
      val index2 = allVars.map(k => if (vm2.contains(k)) vm2(k) else -2)
      val s1 = rearrange(index1, this.score)
      val s2 = rearrange(index2, that.score)
      val v1 = rearrange(index1, this.variance)
      val v2 = rearrange(index2, that.variance)
      this.binary match {
        case false => DefaultRMWResult(binary = false, sizes, allVars, s1 + s2, v1 + v2)
        case true => DefaultRMWResult(binary = true, sizes, allVars, s1 + s2, v1 + v2)
      }
    }

    def conditional(known: Array[Variation]): RMWResult = {
      lazy val local = known.toSet.intersect(vars.toSet)

      if (known.isEmpty || local.isEmpty) {
        this
      } else {
        val n1 = local.size
        val index1 = vars.zipWithIndex.filter(p => local.contains(p._1)).map(_._2)
        val index2 = vars.zipWithIndex.filter(p => ! local.contains(p._1)).map(_._2)
        val index = index1 ++ index2
        val newVars = index2.map(i => vars(i))
        val u = rearrange(index, score)
        val u1 = u(0 until n1)
        val u2 = u(n1 until u.length)
        val v = rearrange(index, variance)
        val v11Inv = inv(v(0 until n1, 0 until n1))
        val v12 = v(0 until n1, n1 until v.cols)
        val v21 = v(n1 until v.rows, 0 until n1)
        val v22 = v(n1 until v.rows, n1 until v.cols)
        this.binary match {
          case false =>
            val numSample = getMaf(vars.head)._2
            val aa = 1.0 - u1.t * v11Inv * u1 / numSample
            val newScore = (u2 - v21 * v11Inv * u1)/aa
            val newVariance = (v22 - v21 * v11Inv * v12)/aa
            DefaultRMWResult(binary = false, this.sampleSizes, newVars, newScore, newVariance)
          case true =>
            val newScore = u2 - v21 * v11Inv * u1
            val newVariance = v22 - v21 * v11Inv * v12
            DefaultRMWResult(binary = true, this.sampleSizes, newVars, newScore, newVariance)
        }
      }
    }
  }

  case class DefaultRMWResult(binary: Boolean,
                              sampleSizes: Array[Int],
                              vars: Array[Variation],
                              score: DV[Double],
                              variance: DM[Double]) extends RMWResult {
    def header = ""
    def pValue: Option[Double] = Some(1.0)
    def statistic: Double = 0.0
    def self: DefaultRMWResult = this
  }


  def getMaf(v: Variation): (Int, Int) = {
    val regex = s"${IK.mac}=(\\d+),(\\d+)".r
    v.info.get match {
      case regex(ac, an) => (ac.toInt, an.toInt)
      case _ => (0, 0)
    }
  }

  def commonAndRare(v1: Option[Array[Variation]], v2: Option[Array[Variation]]): Array[Variation] = {
    (v1, v2) match {
      case (None, None) => Array[Variation]()
      case (Some(x), None) => x
      case (None, Some(y)) => y
      case (Some(x), Some(y)) => x ++ y
    }
  }

  def mergeVars(v1: Array[Variation], v2: Array[Variation]): Array[Variation] = {
    val vm1 = v1.map{v =>
      val mc = v.info.get.split(",").map(_.toInt)
      v -> (mc(0), mc(1))
    }.toMap
    val vm2 = v2.map{v =>
      val mc = v.info.get.split(",").map(_.toInt)
      v -> (mc(0), mc(1))
    }.toMap
    val res = vm1 ++ (for ((v, m) <- vm2) yield if (vm1.contains(v)) v -> cesg.PairInt.op(m, vm1(v)) else v -> m)
    res.map{
      case (v, m) =>
        v.info = Some(s"${IK.mac}=${m._1},${m._2}")
        v
    }.toArray.sorted
  }

  def rearrange(index: Array[Int], data: DV[Double]): DV[Double] = {
    val res = index.map(i => if (i == -1) 0.0 else data(i))
    DV(res: _*)
  }

  def rearrange(index: Array[Int], data: DM[Double]): DM[Double] = {
    val res = index.map{i =>
      if (i == -1) {
        DV.zeros[Double](index.length)
      } else {
        rearrange(index, data(::, i))
      }
    }
    DV.horzcat(res: _*)
  }

}
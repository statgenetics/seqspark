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

import breeze.linalg.{DenseMatrix => DM, DenseVector => DV, _}
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.stat.LCCSLiu.CDFLiu
import org.dizhang.seqspark.stat.LCCSResampling._
import org.dizhang.seqspark.util.General._
/**
  * This is also a moment matching method
  * to approximate LCCS with a Chi-squared distribution
  * Comparing with Liu method, it uses different formula to compute Variance
  * and uses resampling method to compute Kurtosis
  *
  */
@SerialVersionUID(7778570001L)
class LCCSResampling(val lambda: DV[Double],
                     u: DM[Double],
                     residualsVariance: DV[Double],
                     qscores: DV[Double]) extends LinearCombinationChiSquare {
  def nonCentrality = DV.zeros[Double](lambda.length)
  def degreeOfFreedom = DV.ones[Double](lambda.length)
  def muQ = sum(lambda)
  def varQ = getVar(lambda, residualsVariance, u)
  def df = getDF(qscores)
  def kurQ = getKurtosis(qscores)
  def cdf(cutoff: Double): CDFLiu = {
    val cs = new ChiSquared(df)
    val norm = (cutoff - muQ) * (2 * df).sqrt/varQ.sqrt + df
    CDFLiu(cs.cdf(norm), 0)
  }
}

object LCCSResampling {
  def varElement(m4: DV[Double],
                 u1: DV[Double],
                 u2: DV[Double]): Double = {
    val tmp = pow(u1, 2) *:* pow(u2, 2)
    val a1 = m4 dot tmp
    val a2 = sum(pow(u1,2)) * sum(pow(u2, 2)) - sum(tmp)
    val a3 = (u1 dot u2).square - sum(tmp)
    a1 + a2 + 2*a3
  }
  def getSkewness(qs: DV[Double]): Double = {
    val m3 = mean(pow(qs - mean(qs), 3))
    m3/stddev(qs).cube
  }

  def getKurtosis(qs: DV[Double]): Double = {
    if (stddev(qs) > 0) {
      val m4 = mean(pow(qs - mean(qs), 4))
      m4 / stddev(qs).square.square - 3
    } else {
      -100.0
    }
  }

  def getDF(qscores: DV[Double]): Double = {
    val s2 = getKurtosis(qscores)
    val df = 12.0/s2
    if (s2 < 0) {
      100000.0
    } else if (df < 0.01) {
      val s1 = getSkewness(qscores)
      8.0/s1.square
    } else {
      df
    }
  }

  def getVar(lambda: DV[Double],
             residualsVariance: DV[Double],
             u: DM[Double]): Double = {
    val n = lambda.length
    val m4 = residualsVariance.map(rv => rv * (3 * rv + 1) / rv.square)
    val zeta = DV((
      for {
        i <- 0 until n
        tmp = sum(pow(u(::, i), 2)).square - sum(pow(u(::,i), 4))
      } yield (m4 dot pow(u(::, i), 4)) + 3 * tmp): _*)
    val cov =
      if (n == 1) {
        DM(zeta(0) * lambda(0).square)
      } else {
        diag(zeta *:* pow(lambda, 2))
      }
    for {
      i <- 0 until (n - 1)
      j <- (i + 1) until n
    } {
      cov(i,j) = varElement(m4, u(::,i), u(::,j))
      cov(i,j) *= lambda(i) * lambda(j)
      cov(j,i) = cov(i,j)
    }
    sum(cov) - sum(lambda).square
  }
}
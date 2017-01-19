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

import breeze.linalg.{*, diag, DenseMatrix => DM, DenseVector => DV}
import com.sun.jna.ptr._
import com.sun.jna.{Library, Native}
import org.dizhang.seqspark.util.General._
import org.slf4j.LoggerFactory

/**
  * multi-variate normal distribution cdf
  * call native fortran library built from Genz et al.
  *
  */
@SerialVersionUID(7778660001L)
trait MultivariateNormal extends Serializable {
  def numVariates: Int = mu.length
  def mu: DV[Double]
  def cov: DM[Double]

  def corr: DM[Double] = {
    val sd = diag(cov).map(x => 1.0/x.sqrt)

    val tmp = cov(::, *) :* sd

    tmp(*, ::) :* sd
  }

  def cdf(cutoff: DV[Double]): MultivariateNormal.CDF = {

    val c = (for (i <- 0 until corr.rows) yield corr(i, ::).t.toArray.mkString(",")).mkString("\n")

    //MultivariateNormal.logger.debug("cor:\n" + c)

    val corrMat = corr
    val corrArr =
      (for {
        i <- 1 until corr.rows
        j <- 0 until i
      } yield corrMat(i, j)).toArray
    //MultivariateNormal.logger.debug(s"cor array: ${corrArr.mkString(",")}")
    val lower = Array.fill(numVariates)(0.0)
    val upper = cutoff //(cutoff - mu) :/ diag(cov).map(x => x.sqrt)
    val infin = Array.fill(numVariates)(0)
    val maxpts = 100000 * numVariates
    val abseps = 1e-10
    val releps = 1e-6
    MultivariateNormal.cdf(numVariates, lower, upper.toArray, infin, corrArr, maxpts, abseps, releps)
  }
}

object MultivariateNormal {

  val logger = LoggerFactory.getLogger(getClass)
  @SerialVersionUID(7778660101L)
  case class CDF(pvalue: Double, error: Double, inform: Int) extends Serializable

  trait MVNImpl extends Library {
    def mvndst_(N: IntByReference,
               LOWER: Array[Double],
               UPPER: Array[Double],
               INFIN: Array[Int],
               CORREL: Array[Double],
               MAXPTS: IntByReference,
               ABSEPS: DoubleByReference,
               RELEPS: DoubleByReference,
               ERROR: DoubleByReference,
               VALUE: DoubleByReference,
               INFORM: IntByReference)
  }

  object MVNImpl {
    def Instance = Native.loadLibrary("mvn", classOf[MVNImpl]).asInstanceOf[MVNImpl]
  }

  def cdf(n: Int, lower: Array[Double], upper: Array[Double], infin: Array[Int],
          correl: Array[Double], maxpts: Int, abseps: Double, releps: Double): CDF = {
    val v: DoubleByReference = new DoubleByReference(0.0)
    val e: DoubleByReference = new DoubleByReference(0.0)
    val i: IntByReference = new IntByReference(0)
    MVNImpl.Instance.mvndst_(new IntByReference(n), lower, upper, infin, correl,
      new IntByReference(maxpts), new DoubleByReference(abseps),
      new DoubleByReference(releps), e, v, i)
    CDF(v.getValue, e.getValue, i.getValue)
  }

  @SerialVersionUID(7778660201L)
  final case class Centered(cov: DM[Double]) extends MultivariateNormal {
    def mu = DV.fill(cov.cols)(0.0)
  }

}

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

import breeze.linalg.{*, CSCMatrix, cholesky, diag, eigSym, sum, DenseMatrix => DM, DenseVector => DV}
import breeze.stats.mean
import org.apache.spark.SparkContext
import org.dizhang.seqspark.assoc.SKAT._
import org.dizhang.seqspark.ds.Variation
import org.dizhang.seqspark.stat.HypoTest.{NullModel => NM}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.General._

import scala.language.existentials
import scala.util.{Success, Try}
/**
  * variance component test
  *
  * use the identical META analysis implementation
  *
  */

object SKAT {

  /**
  def apply(nullModel: NullModel,
            x: CSCMatrix[Double],
            method: String,
            rho: Double): SKAT = {
    val cd = Encode.Rare(x, Array())
    apply(nullModel, cd, method, rho)
  }
  */

  def apply(nullModel: NM,
            x: Encode.Rare,
            method: String,
            rho: Double): SKAT = {
    val nmf = nullModel match {
      case NM.Simple(y, b) => NM.Fit(y, b)
      case NM.Mutiple(y, c, b) => NM.Fit(y, c, b)
      case nm: NM.Fitted => nm
    }

    val st = ScoreTest(nmf, x.coding)

    apply(st, x.vars, method, rho)
  }

  def apply(scoreTest: ScoreTest,
            vars: Array[Variation],
            method: String,
            rho: Double): SKAT = {
    method match {
      case "liu.mod" => LiuModified(scoreTest, vars, rho)
      case "liu" => Liu(scoreTest, vars, rho)
      case _ => Davies(scoreTest, vars, rho)
    }
  }

  def apply(nullModel: NM.Fitted,
            x: Encode.Rare,
            resampled: DM[Double],
            rho: Double): SKAT = {
    SmallSampleAdjust(nullModel, x.asInstanceOf[Encode.Rare], resampled, rho)
  }

  def makeResampled(nullModel: NM.Fitted)(implicit sc: SparkContext): DM[Double] = {
    val times = sc.parallelize(1 to 10000)
    val nm = sc.broadcast(nullModel)
    times.map(i =>
      Resampling.Base(nm.value).makeNewNullModel.residuals.toDenseMatrix
    ).reduce((m1, m2) => DM.vertcat(m1, m2))
  }

  def getMoments(dm: DM[Double]): IndexedSeq[Double] = {
    val dm2 = dm * dm
    val c1 = sum(diag(dm))
    val c2 = sum(diag(dm2))
    val c3 = sum(dm *:* dm2.t)
    val c4 = sum(dm2 *:* dm2.t)
    Vector(c1, c2, c3, c4)
  }

  def getLambda(dm: DM[Double]): Option[DV[Double]] = {
    val egTry = Try(eigSym.justEigenvalues(dm))
    egTry match {
      case Success(ev) =>
        val cutoff = mean(ev) * 1e-6
        val lambda = DV(ev.toArray.filter(v => v > cutoff):_*)
        if (lambda.length == 0)
          None
        else {
          Some(lambda)
        }
      case _ => None
    }
  }

  def getLambdaU(sm: DM[Double]): Option[(DV[Double],DM[Double])] = {
    val egTry = Try(eigSym(sm))
    egTry match {
      case Success(eg) =>
        val vals = eg.eigenvalues
        val cutoff = mean(vals) * 1e-6
        val lambda = DV(vals.toArray.filter(v => v > cutoff): _*)
        if (lambda.length == 0) {
          None
        } else {
          val vecs = eg.eigenvectors
          val u = DV.horzcat(
            (for {
              i <- 0 until vals.length
              if vals(i) > 1e-6
            } yield vecs(::, i)): _*)
          Some((lambda, u))
        }
      case _ =>
        None
    }
  }

  @SerialVersionUID(7727750101L)
  final case class Davies(scoreTest: ScoreTest,
                          vars: Array[Variation],
                          rho: Double = 0.0) extends SKAT {

    def result: AssocMethod.SKATResult = {
      getLambda(vc) match {
        case None =>
          AssocMethod.SKATResult(vars, qScore, None, """method=Davies;failed to compute eigen values""")
        case Some(l) =>
          val cdf = LCCSDavies.Simple(l).cdf(qScore)
          if (cdf.pvalue >= 1.0 || cdf.pvalue <= 0.0) {
            val liucdf = LCCSLiu.Modified(l).cdf(qScore)
            val info = s"method=Liu.mod;DaviesPvalue=${1.0 - cdf.pvalue}"
            AssocMethod.SKATResult(vars, qScore, Some(1.0 - liucdf.pvalue), info)
          } else {
            val info = s"method=Davies;ifault=${cdf.ifault};trace=${cdf.trace.map(_.toInt).mkString(",")}"
            AssocMethod.SKATResult(vars, qScore, Some(1.0 - cdf.pvalue), info)
          }
      }
    }

  }
  @SerialVersionUID(7727750201L)
  final case class Liu(scoreTest: ScoreTest,
                       vars: Array[Variation],
                       rho: Double = 0.0) extends SKAT {

    def result: AssocMethod.SKATResult = {
      val cs = getMoments(vc)
      val cdf = LCCSLiu.SimpleMoments(cs).cdf(qScore)
      if (cdf.ifault == 0)
        AssocMethod.SKATResult(vars, qScore, Some(1 - cdf.pvalue), "method=Liu;success")
      else
        AssocMethod.SKATResult(vars, qScore, None, "method=Liu;fail to get p value")
    }

  }
  @SerialVersionUID(7727750301L)
  final case class LiuModified(scoreTest: ScoreTest,
                               vars: Array[Variation],
                               rho: Double = 0.0) extends SKAT {

    def result: AssocMethod.SKATResult = {
      val cs = getMoments(vc)
      val cdf = LCCSLiu.ModifiedMoments(cs).cdf(qScore)
      if (cdf.ifault == 0)
        AssocMethod.SKATResult(vars, qScore, Some(1 - cdf.pvalue), "method=Liu.mod;success")
      else
        AssocMethod.SKATResult(vars, qScore, None, "method=Liu.mod;fail to get p value")
    }
  }
  @SerialVersionUID(7727750401L)
  final case class SmallSampleAdjust(nullModel: NM.Fitted,
                                     x: Encode.Rare,
                                     resampled: DM[Double],
                                     rho: Double = 0.0) extends SKAT {

    val scoreTest = ScoreTest(nullModel, x.coding)
    val vars = x.vars
    /** resampled is a 10000 x n matrix, storing re-sampled residuals */
    val simScores = resampled * x.coding * L
    val simQs: DV[Double] = simScores(*, ::).map(s => s.t * kernel * s)

    def result: AssocMethod.SKATResult = {
      getLambdaU(vc) match {
        case Some((l, u)) =>
          val cdf = new LCCSResampling(l, u, nullModel.b, simQs).cdf(qScore)
          if (cdf.ifault == 0)
            AssocMethod.SKATResult(x.vars, qScore, Some(1 - cdf.pvalue), "Method=SmallSampleAdjust;success")
          else
            AssocMethod.SKATResult(x.vars, qScore, None, "Method=SmallSampleAdjust;fail to get p value")
        case None =>
          AssocMethod.SKATResult(x.vars, qScore, None, "Method=SmallSampleAdjust;fail to get eigen values and vectors")
      }
    }
  }
}

/**
  * SKAT and SKAT-O are analytic tests
  * The resampling version would be too slow
  * */
@SerialVersionUID(7727750001L)
trait SKAT extends AssocMethod with AssocMethod.AnalyticTest {
  //def nullModel: NM.Fitted
  //def x: Encode.Rare
  def rho: Double

  def vars: Array[Variation]
  /**
    * we trust the size is not very large here
    * otherwise, we could not store the matrix in memory
    *
    * Moved the weight to the encode module
    * */
  lazy val kernel: DM[Double] = {
    val size = scoreTest.score.length
    (1.0 - rho) * DM.eye[Double](size) + rho * DM.ones[Double](size, size)
  }

  lazy val L = cholesky(kernel)

  //def geno = x.coding * cholesky(kernel)
  def scoreTest: ScoreTest //= ScoreTest(nullModel, geno)

  //def phi2 = nullModel.a

  def qScore: Double = {
    val s = (scoreTest.score.toDenseMatrix * L).toDenseVector
    s dot s
    //scoreTest.score.t * scoreTest.score //* phi2
  }
  //lazy val scoreSigma: DM[Double] = symMatrixSqrt(scoreTest.variance)
  lazy val vc = L.t * scoreTest.variance * L //* phi2 //scoreSigma * kernel * scoreSigma

  def result: AssocMethod.SKATResult
}

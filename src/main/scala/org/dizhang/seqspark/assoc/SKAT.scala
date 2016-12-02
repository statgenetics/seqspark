package org.dizhang.seqspark.assoc

import breeze.linalg.{*, diag, eigSym, DenseMatrix => DM, DenseVector => DV}
import org.apache.spark.SparkContext
import org.dizhang.seqspark.assoc.SKAT._
import org.dizhang.seqspark.stat.ScoreTest.{LinearModel, LogisticModel, NullModel}
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

  def apply(nullModel: NullModel,
            x: Encode[_],
            rho: Double): SKAT = {
    val method = x.config.misc.method
    method match {
      case "liu.mod" => LiuModified(nullModel, x, rho)
      case "liu" => Liu(nullModel, x, rho)
      case _ => Davies(nullModel, x, rho)
    }
  }

  def apply(nullModel: LogisticModel,
            x: Encode[_],
            resampled: DM[Double],
            rho: Double): SKAT = {
    SmallSampleAdjust(nullModel, x, resampled, rho)
  }

  def makeResampled(nullModel: LogisticModel)(implicit sc: SparkContext): DM[Double] = {
    val times = sc.parallelize(1 to 10000)
    val nm = sc.broadcast(nullModel)
    times.map(i =>
      Resampling.Base(nm.value).makeNewNullModel.residuals.toDenseMatrix
    ).reduce((m1, m2) => DM.vertcat(m1, m2))
  }

  def getLambdaU(sm: DM[Double]): (Option[DV[Double]], Option[DM[Double]]) = {
    val egTry = Try(eigSym(sm))

    egTry match {
      case Success(eg) =>
        val vals = eg.eigenvalues
        val vecs = eg.eigenvectors
        val lambda = DV(vals.toArray.filter(v => v > 1e-6): _*)
        if (lambda.length == 0) {
          (None, None)
        } else {
          val u = DV.horzcat(
            (for {
              i <- 0 until vals.length
              if vals(i) > 1e-6
            } yield vecs(::, i)): _*)
          (Some(lambda), Some(u))

        }
      case _ =>
        (None, None)
    }


  }

  @SerialVersionUID(7727750101L)
  final case class Davies(nullModel: NullModel,
                          x: Encode[_],
                          rho: Double = 0.0) extends SKAT {

    def pValue: Option[Double] = {
      lambda.map(l => 1.0 - LCCSDavies.Simple(l).cdf(qScore).pvalue)
    }
  }
  @SerialVersionUID(7727750201L)
  final case class Liu(nullModel: NullModel,
                       x: Encode[_],
                       rho: Double = 0.0) extends SKAT {

    def pValue: Option[Double] = {
      lambda.map(l => 1.0 - LCCSLiu.Simple(l).cdf(qScore).pvalue)
    }
  }
  @SerialVersionUID(7727750301L)
  final case class LiuModified(nullModel: NullModel,
                               x: Encode[_],
                               rho: Double = 0.0) extends SKAT {

    def pValue: Option[Double] = {

      lambda match {
        case None => None
        case Some(l) =>
          val cdf = LCCSLiu.Modified(l).cdf(qScore)
          if (cdf.ifault != 0.0)
            None
          else
            Some(1.0 - cdf.pvalue)
      }
    }
  }
  @SerialVersionUID(7727750401L)
  final case class SmallSampleAdjust(nullModel: LogisticModel,
                                     x: Encode[_],
                                     resampled: DM[Double],
                                     rho: Double = 0.0) extends SKAT {
    /** resampled is a 10000 x n matrix, storing re-sampled residuals */
    val simScores = resampled * geno
    val simQs: DV[Double] = simScores(*, ::).map(s => s.t * kernel * s)

    def pValue: Option[Double] = {
      (lambda, u) match {
        case (Some(l), Some(u1)) =>
          val dis = new LCCSResampling(l, u1, nullModel.residualsVariance, simQs)
          Some(1.0 - dis.cdf(qScore).pvalue)
        case (_, _) =>
          None
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
  def nullModel: NullModel
  def x: Encode[_]
  def isDefined: Boolean = x.isDefined
  def rho: Double
  lazy val weight = DV(x.weight.toArray.zip(x.maf)
    .filter(p => p._2 < x.fixedCutoff || p._2 > (1 - x.fixedCutoff))
    .map(_._1))
  /**
    * we trust the size is not very large here
    * otherwise, we could not store the matrix in memory
    * */
  lazy val kernel: DM[Double] = {
    val size = weight.length
    val r = (1.0 - rho) * DM.eye[Double](size) + rho * DM.ones[Double](size, size)
    diag(weight) * r * diag(weight)
  }
  lazy val resVar: Double = {
    nullModel match {
      case lm: LinearModel => lm.residualsVariance
      case _ => 1.0
    }
  }
  lazy val geno = x.getRare().get.coding
  lazy val scoreTest: ScoreTest = ScoreTest(nullModel, geno)

  def qScore: Double = {
    scoreTest.score.t * kernel * scoreTest.score
  }
  lazy val scoreSigma: DM[Double] = symMatrixSqrt(scoreTest.variance)
  lazy val vc = scoreSigma * kernel * scoreSigma
  lazy val (lambda, u) = getLambdaU(vc)
  def pValue: Option[Double]
  def result = AssocMethod.AnalyticResult(x.getRare().get.vars, qScore, pValue)
}

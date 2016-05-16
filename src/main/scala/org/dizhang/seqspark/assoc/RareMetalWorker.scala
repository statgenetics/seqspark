package org.dizhang.seqspark.assoc

import breeze.linalg.{*, sum, DenseMatrix => DM, DenseVector => DV, SparseVector => SV, Vector => Vec}
import breeze.numerics.pow
import org.dizhang.seqspark.stat.{LinearRegression, LogisticRegression, ScoreTest}

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
sealed trait RareMetalWorker {
  def x: Encode
  def score: DV[Double]
  def variance: DM[Double]
}

object RareMetalWorker {

  final case class Default(nullModel: ScoreTest.NullModel,
                           x: Encode) extends RareMetalWorker {
    val common = x.getCommon()
    val rare = x.getRare()
    val model = ScoreTest(nullModel, common.map(_.coding), rare.map(_.coding))
    val score = model.score
    val variance = model.variance
  }
}
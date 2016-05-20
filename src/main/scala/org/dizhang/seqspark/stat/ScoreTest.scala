package org.dizhang.seqspark.stat

import breeze.linalg.{*, CSCMatrix, DenseMatrix, DenseVector, inv, sum}
import breeze.numerics.pow
import org.dizhang.seqspark.stat.ScoreTest.NullModel

/**
  * score test for regression model
  *
  * Here we only compute the score vectors and its cov matrix
  * leave the p-value to specific association method, e.g.
  * Burden, VT, MetaAnalysis summary
  *
  * for efficiency, avoid CSCMatrix * DenseMatrix,
  * instead, use DenseMatrix * CSCMatrix
  *
  * The SparseXXX classes are using SparseMatrix to coding rare variants,
  * so this test is very fast for large sample rare variant association
  *
  * Only implement linear and logistic models here
  *
  */

object ScoreTest {

  @SerialVersionUID(52L)
  trait NullModel extends Serializable {
    def regressionResult: Regression.Result
    def xs = regressionResult.xs
    val residuals = regressionResult.responses - regressionResult.estimates
    val informationInverse = inv(regressionResult.information)
  }
  case class LinearModel(regressionResult: Regression.LinearResult) extends NullModel {
    val residualsVariance = sum(pow(regressionResult.residuals, 2))/regressionResult.residuals.length
  }
  case class LogisticModel(regressionResult: Regression.LogisticResult) extends NullModel {
    val residualsVariance = regressionResult.estimates.map(p => p * (1.0 - p))
    val xsRV = (xs(::, *) :* residualsVariance).t
  }

  case object Dummy extends ScoreTest {
    def score = DenseVector(0.0)
    def variance = DenseMatrix(0.0)
  }

  def apply(nm: NullModel, x: CSCMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(_) => SparseContinuous(nm.asInstanceOf[LinearModel], x)
      case LogisticModel(_) => SparseDichotomous(nm.asInstanceOf[LogisticModel], x)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel, x: DenseMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(_) => DenseContinuous(nm.asInstanceOf[LinearModel], x)
      case LogisticModel(_) => DenseDichotomous(nm.asInstanceOf[LogisticModel], x)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel, x: DenseVector[Double]): ScoreTest = {
    nm match {
      case LinearModel(_) => DenseContinuous(nm.asInstanceOf[LinearModel], x.toDenseMatrix.t)
      case LogisticModel(_) => DenseDichotomous(nm.asInstanceOf[LogisticModel], x.toDenseMatrix.t)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel,
            x1: DenseMatrix[Double],
            x2: CSCMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(_) => MixedContinuous(nm.asInstanceOf[LinearModel], x1, x2)
      case LogisticModel(_) => MixedDichotomous(nm.asInstanceOf[LogisticModel], x1, x2)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel,
            x1: Option[DenseMatrix[Double]],
            x2: Option[CSCMatrix[Double]]): ScoreTest = {
    (x1, x2) match {
      case (Some(c), Some(r)) => apply(nm, c, r)
      case (Some(c), None) => apply(nm, c)
      case (None, Some(r)) => apply(nm, r)
      case (None, None) => Dummy
    }
  }


  final case class SparseContinuous(nullModel: LinearModel,
                                    x: CSCMatrix[Double]) extends ScoreTest {
    val score = (nullModel.residuals.toDenseMatrix * x).toDenseVector / nullModel.residualsVariance
    lazy val variance = {
      val c = nullModel.xs
      val resVar = nullModel.residualsVariance
      val IccInv = nullModel.informationInverse * resVar
      val Igg = (x.t * x).toDense
      /** this is important here */
      val Icg = c.t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg)/resVar
    }
  }

  final case class DenseContinuous(nullModel: LinearModel,
                                   x: DenseMatrix[Double]) extends ScoreTest {
    /** Because x is dense here. it will take a longer time to run
      * only use this for common variants
      * */
    val score = (x.t * nullModel.residuals) / nullModel.residualsVariance
    lazy val variance = {
      val c = nullModel.xs
      val resVar = nullModel.residualsVariance
      /**
        * theoretically, each information matrix here is lack of a resVar term.
        *
        * */
      val IccInv = nullModel.informationInverse * resVar
      val Igg = x.t * x
      val Icg = c.t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg)/resVar
    }
  }

  final case class MixedContinuous(nullModel: LinearModel,
                                   x1: DenseMatrix[Double],
                                   x2: CSCMatrix[Double]) extends ScoreTest {
    /** to efficiently compute the covariance matrix,
      * we need to blockwise the genotype matrix first
      * */
    private val dense = DenseContinuous(nullModel, x1)
    private val sparse = SparseContinuous(nullModel, x2)
    val score = DenseVector.vertcat(dense.score, sparse.score)
    lazy val variance = {
      val v1 = dense.variance
      val v4 = sparse.variance
      val v2 = {
        val c = nullModel.xs
        val IccInv = nullModel.informationInverse * nullModel.residualsVariance
        val Igg = x1.t * x2
        val Icg = c.t * x2
        val Igc = x1.t * c
        (Igg - Igc * IccInv * Icg)/nullModel.residualsVariance
      }
      val v3 = v2.t
      val v12 = DenseMatrix.horzcat(v1, v2)
      val v34 = DenseMatrix.horzcat(v3, v4)
      DenseMatrix.vertcat(v12, v34)
    }
  }

  final case class SparseDichotomous(nullModel: LogisticModel,
                                     x: CSCMatrix[Double]) extends ScoreTest {
    val score = (nullModel.residuals.toDenseMatrix * x).toDenseVector
    lazy val variance = {
      val IccInv = nullModel.informationInverse
      val Igg = (x.t * x).toDense
      val Icg = nullModel.xsRV * x
      val Igc = Icg.t
      Igg - Igc * IccInv * Icg
    }
  }

  final case class DenseDichotomous(nullModel: LogisticModel,
                                    x: DenseMatrix[Double]) extends ScoreTest {
    val score = x.t * nullModel.residuals
    lazy val variance = {
      val IccInv = nullModel.informationInverse
      val Igg = x.t * x
      val Icg = nullModel.xsRV * x
      val Igc = Icg.t
      Igg - Igc * IccInv * Icg
    }
  }

  final case class MixedDichotomous(nullModel: LogisticModel,
                                    x1: DenseMatrix[Double],
                                    x2: CSCMatrix[Double]) extends ScoreTest {
    private val dense = DenseDichotomous(nullModel, x1)
    private val sparse = SparseDichotomous(nullModel, x2)
    val score = DenseVector.vertcat(dense.score, sparse.score)
    lazy val variance = {
      val v1 = dense.variance
      val v4 = sparse.variance
      val v2 = {
        val IccInv = nullModel.informationInverse
        val Igg = x1.t * x2
        val Icg = nullModel.xsRV * x2
        val Igc = x1.t * nullModel.xsRV.t
        Igg - Igc * IccInv * Icg
      }
      val v3 = v2.t
      val v12 = DenseMatrix.horzcat(v1, v2)
      val v34 = DenseMatrix.horzcat(v3, v4)
      DenseMatrix.vertcat(v12, v34)
    }
  }

}

sealed trait ScoreTest extends HypoTest {
  def score: DenseVector[Double]
  def variance: DenseMatrix[Double]
}

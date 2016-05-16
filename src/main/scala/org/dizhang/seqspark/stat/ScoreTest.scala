package org.dizhang.seqspark.stat

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector}

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
  sealed trait NullModel extends Serializable {
    def regressionResult: Regression.Result
  }
  final case class LinearModel(regressionResult: Regression.LinearResult) extends NullModel
  final case class LogisticModel(regressionResult: Regression.LogisticResult) extends NullModel

  case object Dummy extends ScoreTest {
    def score = DenseVector(0.0)
    def variance = DenseMatrix(0.0)
  }

  def apply(nm: NullModel, x: CSCMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(lim) => SparseContinuous(lim, x)
      case LogisticModel(lom) => SparseDichotomous(lom, x)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel, x: DenseMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(lim) => DenseContinuous(lim, x)
      case LogisticModel(lom) => DenseDichotomous(lom, x)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel, x: DenseVector[Double]): ScoreTest = {
    nm match {
      case LinearModel(lim) => DenseContinuous(lim, x.toDenseMatrix.t)
      case LogisticModel(lom) => DenseDichotomous(lom, x.toDenseMatrix.t)
      case _ => Dummy
    }
  }

  def apply(nm: NullModel,
            x1: DenseMatrix[Double],
            x2: CSCMatrix[Double]): ScoreTest = {
    nm match {
      case LinearModel(lim) => MixedContinous(lim, x1, x2)
      case LogisticModel(lom) => MixedDichotomous(lom, x1, x2)
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


  final case class SparseContinuous(regRes: Regression.LinearResult,
                                    x: CSCMatrix[Double]) extends ScoreTest {
    val score = (regRes.residuals.toDenseMatrix * x).toDenseVector / regRes.residualsVariance
    val variance = {
      val c = regRes.xs
      val resVar = regRes.residualsVariance
      val IccInv = regRes.informationInverse * resVar
      val Igg = (x.t * x).toDense
      /** this is important here */
      val Icg = c.t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg)/resVar
    }
  }

  final case class DenseContinuous(regRes: Regression.LinearResult,
                                   x: DenseMatrix[Double]) extends ScoreTest {
    /** Because x is dense here. it will take a longer time to run
      * only use this for common variants
      * */
    val score = (x.t * regRes.residuals) / regRes.residualsVariance
    val variance = {
      val c = regRes.xs
      val resVar = regRes.residualsVariance
      /**
        * theoretically, each information matrix here is lack of a resVar term.
        *
        * */
      val IccInv = regRes.informationInverse * resVar
      val Igg = x.t * x
      val Icg = c.t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg)/resVar
    }
  }

  final case class MixedContinous(regRes: Regression.LinearResult,
                                  x1: DenseMatrix[Double],
                                  x2: CSCMatrix[Double]) extends ScoreTest {
    /** to efficiently compute the covariance matrix,
      * we need to blockwise the genotype matrix first
      * */
    private val dense = DenseContinuous(regRes, x1)
    private val sparse = SparseContinuous(regRes, x2)
    val score = DenseVector.vertcat(dense.score, sparse.score)
    val variance = {
      val v1 = dense.variance
      val v4 = sparse.variance
      val v2 = {
        val c = regRes.xs
        val IccInv = regRes.informationInverse * regRes.residualsVariance
        val Igg = x1.t * x2
        val Icg = c.t * x2
        val Igc = x1.t * c
        (Igg - Igc * IccInv * Icg)/regRes.residualsVariance
      }
      val v3 = v2.t
      val v12 = DenseMatrix.horzcat(v1, v2)
      val v34 = DenseMatrix.horzcat(v3, v4)
      DenseMatrix.vertcat(v12, v34)
    }
  }

  final case class SparseDichotomous(regRes: Regression.LogisticResult,
                                     x: CSCMatrix[Double]) extends ScoreTest {
    val score = (regRes.residuals.toDenseMatrix * x).toDenseVector
    val variance = {
      val IccInv = regRes.informationInverse
      val Igg = (x.t * x).toDense
      val Icg = regRes.xsRV * x
      val Igc = Icg.t
      Igg - Igc * IccInv * Icg
    }
  }

  final case class DenseDichotomous(regRes: Regression.LogisticResult,
                                    x: DenseMatrix[Double]) extends ScoreTest {
    val score = x.t * regRes.residuals
    val variance = {
      val IccInv = regRes.informationInverse
      val Igg = x.t * x
      val Icg = regRes.xsRV * x
      val Igc = Icg.t
      Igg - Igc * IccInv * Icg
    }
  }

  final case class MixedDichotomous(regRes: Regression.LogisticResult,
                                    x1: DenseMatrix[Double],
                                    x2: CSCMatrix[Double]) extends ScoreTest {
    private val dense = DenseDichotomous(regRes, x1)
    private val sparse = SparseDichotomous(regRes, x2)
    val score = DenseVector.vertcat(dense.score, sparse.score)
    val variance = {
      val v1 = dense.variance
      val v4 = sparse.variance
      val v2 = {
        val IccInv = regRes.informationInverse
        val Igg = x1.t * x2
        val Icg = regRes.xsRV * x2
        val Igc = x1.t * regRes.xsRV.t
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

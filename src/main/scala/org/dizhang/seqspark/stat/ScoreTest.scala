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

import breeze.linalg.{*, CSCMatrix, DenseMatrix, DenseVector, SparseVector}
import org.dizhang.seqspark.stat.HypoTest.NullModel.{Fitted => SNM}
import org.dizhang.seqspark.util.General._
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
  * The Sparse class uses SparseMatrix to coding rare variants,
  * so this test is very fast for large sample rare variant association
  *
  * Only implement linear and logistic models here
  * They are unified into one model.
  */

object ScoreTest {

  def apply(nm: SNM, x: CSCMatrix[Double]): ScoreTest = {
    Sparse(nm, x)
  }

  def apply(nm: SNM, x: DenseMatrix[Double]): ScoreTest = {
    Dense(nm, x)
  }

  def apply(nm: SNM, x: DenseVector[Double]): ScoreTest = {
    Dense(nm, DenseVector.horzcat(x))
  }

  def apply(nm: SNM, x: SparseVector[Double]): ScoreTest = {
    Sparse(nm, SparseVector.horzcat(x))
  }

  def apply(nm: SNM,
            x1: DenseMatrix[Double],
            x2: CSCMatrix[Double]): ScoreTest = {
    Mixed(nm, x1, x2)
  }

  case class Sparse(nm: SNM,
                    x: CSCMatrix[Double]) extends ScoreTest {
    val score = (nm.residuals.toDenseMatrix * x).toDenseVector / nm.a
    lazy val variance = {
      val c = nm.xs
      val IccInv = nm.invInfo * nm.a
      val Igg = (colMultiply(x, nm.b).t * x).toDense
      val Icg = (c(::, *) *:* nm.b).t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg) / nm.a
    }
  }

  case class Dense(nm: SNM,
                   x: DenseMatrix[Double]) extends ScoreTest {
    val score = x.t * nm.residuals / nm.a
    lazy val variance = {
      val c = nm.xs
      val IccInv = nm.invInfo * nm.a
      val Igg = (x(::, *) *:* nm.b).t * x
      val Icg = (c(::, *) *:* nm.b).t * x
      val Igc = Icg.t
      (Igg - Igc * IccInv * Icg)/nm.a
    }
  }

  case class Mixed(nm: SNM,
                   x1: DenseMatrix[Double],
                   x2: CSCMatrix[Double]) extends ScoreTest {
    private val dense = Dense(nm, x1)
    private val sparse = Sparse(nm, x2)
    val score = DenseVector.vertcat(dense.score, sparse.score)
    lazy val variance = {
      val v1 = dense.variance
      val v4 = sparse.variance
      val v2 = {
        val c = nm.xs
        val IccInv = nm.invInfo * nm.a
        val Igg = (x1(::, *) *:* nm.b).t * x2
        val Icg = (c(::, *) *:* nm.b).t * x2
        val Igc = x1.t * (c(::, *) *:* nm.b).t
        (Igg - Igc * IccInv * Icg) / nm.a
      }
      val v3 = v2.t
      val v12 = DenseMatrix.horzcat(v1, v2)
      val v34 = DenseMatrix.horzcat(v3, v4)
      DenseMatrix.vertcat(v12, v34)
    }
  }

  case class Mock(score: DenseVector[Double],
                  variance: DenseMatrix[Double]) extends ScoreTest
}

@SerialVersionUID(7778780001L)
sealed trait ScoreTest extends HypoTest {
  def score: DenseVector[Double]
  def variance: DenseMatrix[Double]
}

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

import breeze.linalg.{DenseMatrix, DenseVector, inv, *}
import breeze.stats.{mean, variance}

/**
  * hypothesis testing class
  */
@SerialVersionUID(7778480001L)
abstract class HypoTest extends Serializable
object HypoTest {
  sealed trait NullModel {
    def dof: Int
  }


  object NullModel {
    case class Simple(y: DenseVector[Double], binary: Boolean) extends NullModel {
      def dof = y.length
    }
    case class Mutiple(y: DenseVector[Double], x: DenseMatrix[Double], binary: Boolean) extends NullModel {
      def dof = y.length - x.cols
    }

    case class Fitted(y: DenseVector[Double],
                      estimates: DenseVector[Double],
                      xs: DenseMatrix[Double],
                      a: Double,
                      b: DenseVector[Double],
                      binary: Boolean
                      /**
                       * for linear model, a = Var(res), b = DenseVector.fill(xs.rows)(1.0)
                       * for logistic model, a = 1.0, b = estimates * (1 - estimates)
                       * */
                    ) extends NullModel {
      def dof = y.length - xs.cols + 1
      def residuals = y - estimates
      val invInfo = inv(xs.t * (xs(::, *) *:* b) * a)
    }

    def apply(y: DenseVector[Double], x: Option[DenseMatrix[Double]], fit: Boolean, binary: Boolean): NullModel = {
      x match {
        case Some(dm) => apply(y, dm, fit, binary)
        case None => apply(y, fit, binary)
      }
    }

    def apply(y: DenseVector[Double], fit: Boolean, binary: Boolean): NullModel = {
      if (fit) {
        Fit(y, binary)
      } else {
        Simple(y, binary)
      }
    }

    def apply(reg: Regression): NullModel = {
      val y = reg.responses
      reg match {
        case lr: LogisticRegression =>
          Fitted(y, reg.estimates, reg.xs, 1.0, lr.residualsVariance, binary = true)
        case lr: LinearRegression =>
          Fitted(y, reg.estimates, reg.xs, lr.residualsVariance, DenseVector.ones[Double](y.length), binary = false)
      }
    }

    def apply(y: DenseVector[Double], x: DenseMatrix[Double], fit: Boolean, binary: Boolean): NullModel = {
      if (! fit) {
        Mutiple(y, x, binary)
      } else if (binary) {
        val reg = LogisticRegression(y, x)
        Fitted(y, reg.estimates, reg.xs, 1.0, reg.residualsVariance, binary)
      } else {
        val reg = LinearRegression(y, x)
        Fitted(y, reg.estimates, reg.xs, reg.residualsVariance, DenseVector.ones[Double](y.length), binary)
      }
    }

    def Fit(y: DenseVector[Double], x: DenseMatrix[Double], binary: Boolean): Fitted = {
      apply(y, x, fit = true, binary).asInstanceOf[Fitted]
    }

    def Fit(y: DenseVector[Double], binary: Boolean): Fitted = {
      val my = DenseVector.fill(y.length)(mean(y))
      val residuals = y - my
      val xs = DenseMatrix.ones[Double](y.length, 1)
      val invInfo = DenseMatrix.fill(1,1)(1.0/y.length)
      val a = if (binary) 1.0 else variance(residuals)
      val b = if (binary) my.map(e => e * (1 - e)) else DenseVector.ones[Double](y.length)
      Fitted(y, my, xs, a, b, binary)
    }
  }
}

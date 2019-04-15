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

import breeze.linalg.{DenseMatrix, DenseVector, diag, inv}
import breeze.numerics.sqrt
import breeze.stats.distributions.StudentsT
import org.dizhang.seqspark.stat.HypoTest.NullModel
import org.dizhang.seqspark.stat.HypoTest.NullModel._

/**
  * Created by zhangdi on 1/2/17.
  */
trait WaldTest {
  def nm: NullModel
  def x: DenseVector[Double]
  def reg: Regression = {
    nm match {
      case Simple(y, b) =>
        if (b)
          LogisticRegression(y, x.toDenseMatrix.t)
        else
          LinearRegression(y, x.toDenseMatrix.t)
      case Mutiple(y, c, b) =>
        if (b)
          LogisticRegression(y, DenseMatrix.horzcat(x.toDenseMatrix.t, c))
        else
          LinearRegression(y, DenseMatrix.horzcat(x.toDenseMatrix.t, c))
      case Fitted(y, _, xs, _, _, b) =>
        if (b)
          LogisticRegression(y, DenseMatrix.horzcat(x.toDenseMatrix.t, xs(::, 1 until xs.cols)))
        else
          LinearRegression(y, DenseMatrix.horzcat(x.toDenseMatrix.t, xs(::, 1 until xs.cols)))
    }
  }
  def beta: DenseVector[Double] = reg.coefficients
  def std: DenseVector[Double] = {
    sqrt(diag(inv(reg.information)))
  }
  def dof: Int = nm.dof - 1
  def t: DenseVector[Double] = beta /:/ std
  def pValue(oneSided: Boolean = true): DenseVector[Double] = {
    val dis = new StudentsT(dof)
    if (oneSided) {
      t.map(c => 1.0 - dis.cdf(c))
    } else {
      t.map(c => (1.0 - dis.cdf(math.abs(c))) * 2.0)
    }
  }
}

object WaldTest {

  def apply(nm: NullModel, x: DenseVector[Double]): WaldTest = {
    Default(nm, x)
  }

  case class Default(nm: NullModel, x: DenseVector[Double]) extends WaldTest

}
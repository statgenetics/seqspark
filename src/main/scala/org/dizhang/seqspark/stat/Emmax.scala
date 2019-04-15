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
import breeze.linalg.{DenseMatrix=>BDM,CSCMatrix=>BSM,DenseVector=>BDV,SparseVector=>BSV, _}
import breeze.numerics.{exp, pow, log}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix => RM}

/**
  * Emmax implementation of linear mixed models
  * Due to the limitation of the SVD algorithm, sample size cannot be too large
  * e.g. n < 15000
  */

trait Emmax {

}

object Emmax {

  def eigenH(K: BDM[Double]): (BDV[Double], BDM[Double]) = {
    val res = eigSym(K)
    (res.eigenvalues - 1.0, res.eigenvectors)
  }

  def eigenSHS(K: BDM[Double], X: BDM[Double]): (BDV[Double], BDM[Double]) = {
    val n = X.rows
    val q = X.cols
    val Xt = X.t
    val S = BDM.eye[Double](n) - X * inv(Xt * X) * Xt
    val res = eigSym(S * (K + BDM.eye[Double](n)) * S)

    (res.eigenvalues(0 until (n-q)) - 1.0, res.eigenvectors(::, 0 until (n-q)))
  }

  def mle(y: BDV[Double], X: BDM[Double], K: BDM[Double],
          ngrids :Int =100, llim: Int = -10, ulim: Int =10, esp: Double = 1e-10) = {
    val n: Int = y.length
    val t = K.rows
    val q = X.cols
    val eigH = eigenH(K)
    val eigSHS = eigenSHS(K, X)

    val etas: BDV[Double] = (y.t * eigSHS._2).t
    val logDelta: BDV[Double] = BDV((0 to ngrids).map(x => x/ngrids.toDouble * (ulim - llim) + llim): _*)
    val m = logDelta.length
    val delta: BDV[Double] = exp(logDelta)
    val lambdas: BDM[Double] = tile(eigSHS._1, 1, m) + tile(delta, 1, n - q).t
    val xis: BDM[Double] = tile(eigH._1, 1, m) + tile(delta, 1, n).t
    val etasq: BDM[Double] = tile(pow(etas, 2), 1, n-q).t
    val ll: BDV[Double] = 0.5 * (n.toDouble * (log(n/(2*math.Pi)) - 1 - log(sum(etasq /:/ lambdas, Axis._0).t))
      - sum(log(xis), Axis._0).t )
    val dLl: BDV[Double] = 0.5 * delta *:* (n.toDouble * sum(etasq /:/ pow(lambdas, 2), Axis._0).t
      /:/ sum(etasq /:/ lambdas, Axis._0).t - sum(1.0 / xis, Axis._0).t)
    
  }

}

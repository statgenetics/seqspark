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

package org.dizhang.seqspark.ds

import breeze.linalg.{DenseMatrix, DenseVector, inv}

/**
  * type safe matrix, from Evan R. Sparks
  * http://etrain.github.io/2015/05/28/type-safe-linear-algebra-in-scala/
  * With user provided (fake) dimension check
  */

object SafeMatrix {
  def apply[A, B](mat: DenseMatrix[Double]): SafeMatrix[A, B] = {
    new SafeMatrix[A, B](mat)
  }
  def apply[A, B](vec: DenseVector[Double]): SafeMatrix[A, B] = {
    val mat = vec.toDenseMatrix.t
    apply[A, B](mat)
  }
  def inverse[A](x: SafeMatrix[A,A]): SafeMatrix[A,A] = new SafeMatrix[A,A](inv(x.mat))
}

class SafeMatrix[A, B](val mat: DenseMatrix[Double]) {
  def apply(i: Int, j: Int): Double = mat(i,j)

  def *[C](other: SafeMatrix[B, C]): SafeMatrix[A, C] = new SafeMatrix[A, C](mat * other.mat)

  def t: SafeMatrix[B, A] = new SafeMatrix[B, A](mat.t)

  def +(other: SafeMatrix[A, B]): SafeMatrix[A, B] = new SafeMatrix[A, B](mat + other.mat)

  def -(other: SafeMatrix[A, B]): SafeMatrix[A, B] = new SafeMatrix[A, B](mat - other.mat)

  def *:*(other: SafeMatrix[A, B]): SafeMatrix[A, B] = new SafeMatrix[A, B](mat *:* other.mat)

  def *(scalar: Double): SafeMatrix[A, B] = new SafeMatrix[A, B](mat * scalar)
}
package org.dizhang.seqa.ds

import breeze.linalg.{inv, DenseVector, DenseMatrix}

/**
  * type safe matrix, from Evan R. Sparks
  * http://etrain.github.io/2015/05/28/type-safe-linear-algebra-in-scala/
  * With user provided (fake) dimension check
  */

object MyMatrix {
  def apply[A, B](mat: DenseMatrix[Double]): MyMatrix[A, B] = {
    new MyMatrix[A, B](mat)
  }
  def apply[A, B](vec: DenseVector[Double]): MyMatrix[A, B] = {
    val mat = vec.toDenseMatrix.t
    apply[A, B](mat)
  }
  def inverse[A](x: MyMatrix[A,A]): MyMatrix[A,A] = new MyMatrix[A,A](inv(x.mat))
}

class MyMatrix[A, B](val mat: DenseMatrix[Double]) {
  def *[C](other: MyMatrix[B,C]): MyMatrix[A,C] = new MyMatrix[A,C](mat*other.mat)
  def t: MyMatrix[B,A] = new MyMatrix[B,A](mat.t)
  def +(other: MyMatrix[A,B]): MyMatrix[A,B] = new MyMatrix[A,B](mat + other.mat)
  def -(other: MyMatrix[A,B]): MyMatrix[A,B] = new MyMatrix[A,B](mat - other.mat)
  def :*(other: MyMatrix[A,B]): MyMatrix[A,B] = new MyMatrix[A,B](mat :* other.mat)
  def *(scalar: Double): MyMatrix[A,B] = new MyMatrix[A,B](mat * scalar)
}

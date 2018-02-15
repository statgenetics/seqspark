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

import breeze.linalg.{DenseVector, SparseVector, VectorBuilder}

import scala.reflect.ClassTag

//import collection.mutable.{IndexedSeq, Map}
//import scala.collection.mutable

/**
 * This class borrows a lot from the AlgeBird AdaptiveVector
 */

object Counter {


  /** Start define some functions for Counter */

  val THRESHOLD = 0.25
  val MINIMIUM = 1000
  def fill[A](size: Int)(sparseValue: A): Counter[A] = SparseCounter[A](Map.empty[Int, A], sparseValue, size)
  def fromIndexedSeq[A](iseq: IndexedSeq[A], default: A): Counter[A] = {
    if (iseq.isEmpty) {
      fill[A](0)(default)
    } else {
      val denseSize = iseq.count( _ != default)
      if (iseq.size >= MINIMIUM && denseSize < iseq.size * THRESHOLD)
        SparseCounter(toMap(iseq, default), default, iseq.size)
      else
        DenseCounter(iseq, default)
    }
  }

  def fromMap[A](m: Map[Int, A], default: A, size: Int): Counter[A] = {
    if (m.isEmpty)
      fill[A](size)(default)
    else {
      val maxIdx = m.keys.max
      require(maxIdx < size)
      val denseSize = m.count(_._2 != default)
      if (size >= MINIMIUM && denseSize < size * THRESHOLD)
        SparseCounter(m, default, size)
      else
        DenseCounter(toIndexedSeq(m, default, size), default)
    }
  }

  def toMap[A](iseq: IndexedSeq[A], default: A): Map[Int, A] =
    iseq.view.zipWithIndex.filter(_._1 != default).map(_.swap).toMap

  def toIndexedSeq[A](m: Map[Int, A], default: A, size: Int): IndexedSeq[A] = {
    import scala.collection.mutable
    val buf = mutable.Buffer.fill[A](size)(default)
    m.foreach { case (idx, v) => buf(idx) = v }
    Vector(buf: _*)
  }


  def addByKey[A, B](x: Map[B, A], y: Map[B, A])
                (implicit sg: SemiGroup[A]): Map[B, A] = {
    x ++ (for ((k, v) <- y) yield k -> sg.op(x.getOrElse(k, sg.zero), v))
  }

  /**
  *def addGeno[A: ClassTag](g1: Array[A], g2: Array[A])(implicit sg: CounterElementSemiGroup[A]): Array[A] = {
    *for (i <- (0 until g1.length).toArray) yield sg.op(g1(i), g2(i))
  *}
  */
}

@SerialVersionUID(7737260001L)
sealed trait Counter[A] extends Serializable {

  def default: A
  def size: Int
  //def denseSize: Int
  def length = size
  def apply(i: Int): A
  def reduce(implicit sg: SemiGroup[A]): A
  def reduceByKey[B](keyFunc: Int => B)(implicit sg: SemiGroup[A]): Map[B, A]

  def ++(that: Counter[A])(implicit sg: SemiGroup[A]): Counter[A]

  def map[B](f: A => B): Counter[B]

  def toDenseVector(make: A => Double): DenseVector[Double] = {
    DenseVector(toIndexedSeq.map(make(_)): _*)
  }

  def toSparseVector(make: A => Double): SparseVector[Double] = {
    val builder = new VectorBuilder[Double](this.length)
    this.toMap.foreach{case (i, v) => builder.add(i, make(v))}
    builder.toSparseVector
  }

  def toIndexedSeq = this match {
    case DenseCounter(e, _) => e
    case SparseCounter(e, d, s) => Counter.toIndexedSeq(e, d, s)
  }

  def toArray(implicit tag: ClassTag[A]) = this.toIndexedSeq.toArray

  def toMap: Map[Int, A]

}

@SerialVersionUID(7737260201L)
case class DenseCounter[A](elems: IndexedSeq[A], default: A)
  extends Counter[A] {
  def apply(i: Int) = elems(i)
  def size = elems.length
  def reduce(implicit sg: SemiGroup[A]): A =
    elems.reduce((a, b) => sg.op(a, b))
  def reduceByKey[B](keyFunc: Int => B)(implicit sg: SemiGroup[A]): Map[B, A] = {
    elems.view.zipWithIndex.groupBy(x => keyFunc(x._2)).mapValues(
      c => c.map(x => x._1).reduce((a, b) => sg.op(a, b))
    ).map(identity)
  }

  def map[B](f: A => B): Counter[B] = {
    Counter.fromIndexedSeq(elems.map(f), f(default))
  }

  def ++(that: Counter[A])(implicit sg: SemiGroup[A]): Counter[A] = {
    require(this.length == that.length)
    val newElems = this.elems.zip(that.toIndexedSeq).map(x => sg.op(x._1, x._2))
    Counter.fromIndexedSeq(newElems, sg.op(this.default, that.default))
  }

  def toMap = Counter.toMap(toIndexedSeq, default)

}

@SerialVersionUID(7737260301L)
case class SparseCounter[A](elems: Map[Int, A], default: A, size: Int)
  extends Counter[A] {
  def apply(i: Int) = {
    require(i < size)
    elems.getOrElse(i, default)
  }
  def reduce(implicit sg: SemiGroup[A]): A = {
    val dense = elems.values.fold(sg.zero)((a, b) => sg.op(a, b))
    val sparse = sg.pow(default, size - elems.size)
    sg.op(dense, sparse)
  }
  def reduceByKey[B](keyFunc: Int => B)(implicit sg: SemiGroup[A]): Map[B, A] = {

    val dense: Map[B, A] = elems.groupBy(x => keyFunc(x._1)).mapValues(
      c => c.values.reduce((a, b) => sg.op(a, b))
    ).map(identity)
    val denseSizes: Map[B, Int] = elems.groupBy(x => keyFunc(x._1)).mapValues(_.size).map(identity)
    val sizes: Map[B, Int] = (0 until size).groupBy(keyFunc(_)).mapValues(_.size).map(identity)
    val sparseSizes: Map[B, Int] =
      for ((k, s) <- sizes) yield k -> (s - denseSizes.getOrElse(k, 0))
    val sparse = sparseSizes.map(x => x._1 -> sg.pow(default, x._2))
    dense ++ (for ((k, v) <- sparse) yield k -> sg.op(v, dense.getOrElse(k, sg.zero)))
  }

  def map[B](f: A => B): Counter[B] = {
    Counter.fromMap(elems.map(p => p._1 -> f(p._2)), f(default), size)
  }

  def ++(that: Counter[A])(implicit sg: SemiGroup[A]): Counter[A] = {
    //x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0)))
    require(this.length == that.length)
    val newElems = this.elems ++ (for ((k, v) <- that.toMap) yield k -> sg.op(this(k), v))
    Counter.fromMap(newElems, sg.op(this.default, that.default), length)
  }

  def toMap = elems

}



















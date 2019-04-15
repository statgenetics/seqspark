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

package org.dizhang.seqspark.util

import breeze.linalg.{*, CSCMatrix, DenseVector, eigSym, sum, DenseMatrix => DM}
import breeze.numerics._
import breeze.linalg.linspace
import scala.annotation.tailrec

/**
  * some useful functions
  */
object General {

  def badArgs(args: Array[String]): Boolean = {
    if (args.length != 1) {
      true
    } else {
      false
    }
  }

  def usage(mode: String): Unit = {
    val mesg = s"""
                  |spark-submit --class $mode [options] /path/to/wesqc.xx.xx.jar project.conf
                  |
                |   options:         Spark options, e.g. --num-executors, please refer to the spark documentation.
                  |
                |   project.conf:    The configuration file in INI format, could be other name.

               """
    print(mesg)
  }

  implicit class DoubleDouble(val x: (Double, Double)) extends AnyVal {
    def ratio: Double = x._1/x._2
  }

  implicit class RichDouble(val x: Double) extends AnyVal {
    def sqrt = math.pow(x, 0.5)
    def square = math.pow(x, 2.0)
    def cube = math.pow(x, 3.0)
  }

  def max[A](x: A, y: A)(implicit ordering: Ordering[A]): A = {
    ordering.compare(x, y) match {
      case -1 => y
      case _ => x
    }
  }

  def max[A](elems: A*)(implicit ordering: Ordering[A]): A = {
    elems.max
  }

  def colMultiply(cm: CSCMatrix[Double],
                  dv: DenseVector[Double]): CSCMatrix[Double] = {
    require(cm.rows == dv.length, "length of the dense vector must equal rows of the matrix")
    val builder = new CSCMatrix.Builder[Double](cm.rows, cm.cols)
    cm.activeIterator.foreach{
      case ((r,c), v) => builder.add(r, c, v * dv(r))
    }
    builder.result
  }

  def rowMultiply(cm: CSCMatrix[Double],
                  dv: DenseVector[Double]): CSCMatrix[Double] = {
    require(cm.cols == dv.length, "length of the dense vector must equal to columns of the matrix")
    val builder = new CSCMatrix.Builder[Double](cm.rows, cm.cols)
    cm.activeIterator.foreach{
      case ((r,c), v) => builder.add(r,c, v * dv(c))
    }
    builder.result
  }
  def symMatrixSqrt(sm: DM[Double]): DM[Double] = {

    val de = eigSym(sm)
    val values = de.eigenvalues
    val vectors = de.eigenvectors
    (vectors(*, ::) *:* pow(values, 0.5)) * vectors.t
  }

  def simpson2(h: Double, fa: Double, fm: Double, fb: Double): Double = {
    (fa + 4* fm + fb) * h/3
  }
  def simpsonN(h: Double, fn: DenseVector[Double]): Double = {
    val n = fn.length
    val s = fn(0) + fn(-1) + 2.0 * sum(fn(Range(2, n, 2))) + 4.0 * sum(fn(Range(1,n,2)))
    s * h / 3
  }

  def simpsonScan(cur: (Double, Double),
                  fun: DenseVector[Double] => DenseVector[Double],
                  cells: Int,
                  tol: Double): (Double, List[(Double, Double)]) = {
    require(cells >= 32 && cells%16 == 0)
    val a = cur._1
    val b = cur._2
    val h = (b-a)/cells
    val fn = fun(linspace(a, b, cells + 1))
    //println(s"${fn.toArray.zipWithIndex.filter(_._1 != 0.0).map(p => s"(${p._1},${a + p._2 * h})").mkString("::")}")

    /**
    val left = simpsonN(2*h, fn(Range(0, cells/2 + 1, 2)))
    val leftLeft = simpsonN(h,fn(Range(0, cells/4 + 1)))
    val leftRight = simpsonN(h, fn(Range(cells/4, cells/2 + 1)))
    val right = simpsonN(2*h, fn(Range(cells/2, cells + 1, 2)))
    val rightLeft = simpsonN(h, fn(Range(cells/2, cells * 3/ 4 + 1)))
    val rightRight = simpsonN(h, fn(Range(cells * 3/4, cells + 1)))
    var intervals: List[(Double, Double)] = Nil
    var acc = 0.0
    if (abs(leftLeft + leftRight - left) > tol) {
      intervals = (a, (a+b)/2) :: intervals
    } else {
      acc += left
    }
    if (abs(rightLeft + rightRight - right) > tol) {
      intervals = ((a+b)/2, b) :: intervals
    } else {
      acc += right
    }
    (acc, intervals)

      */

    var i = 0
    var acc = 0.0
    var intervals: List[(Int, Int)] = Nil
    val step = cells/4
    while (i < 4) {
      val left = simpsonN(h, fn(Range(i * step, i * step + step/2 + 1, 1)))
      val right = simpsonN(h, fn(Range(i * step + step/2, i * step + step + 1, 1)))
      val all = simpsonN(2*h, fn(Range(i * step, i * step + step + 1, 2)))
      //println(s"i: $i left: $left right: $right all: $all")
      if (abs(left + right - all) > tol) {
        intervals =
          intervals match {
            case Nil => (i, i + 1) :: intervals
            case l => (i, i+1) :: l
          }
      } else {
        acc += all
      }
      i += 1
    }
    (acc, intervals.map(p => (a + step * h * p._1, a + step * h * p._2)))


  }

  def quadratureScan(fun: DenseVector[Double] => DenseVector[Double],
                     a: Double, b: Double,
                     cells: Int = 80,
                     maxLevel: Int = 1000,
                     tol: Double = 1e-6): Option[Double] = {
    @tailrec
    def quadra(intervals: List[(Double, Double)], acc: Double, level: Int): Option[Double] = {
      if (level > maxLevel) {
        None
      } else {
        if (intervals.isEmpty) {
          Some(acc)
        } else {
          val cur = intervals.head
          val res = simpsonScan(cur, fun, cells, tol)
          //println(s"level: $level res: ${res._1 + acc} intervals: ${(res._2 ::: intervals.tail).mkString("::")}")
          quadra(intervals.tail ::: res._2, acc + res._1, level + 1)
        }
      }
    }
    quadra(List(a -> b), 0.0, 0)
  }

  def quadratureN(fun: DenseVector[Double] => DenseVector[Double],
                  a: Double, b: Double,
                  cells: Int = 80,
                  maxLevel: Int = 2000,
                  tol: Double = 1e-6): Option[Double] = {
    @tailrec
    def quadra(intervals: List[(Double, Double, Double)], acc: Double, level: Int): Option[Double] = {
      if (level > maxLevel) {
        None
      } else {
        if (intervals.isEmpty) {
          Some(acc)
        } else {
          //println(s"old level: $level res: $acc intervals: ${intervals.mkString("::")}")
          val cur = intervals.head
          val a = cur._1
          val b = cur._2
          val m = (a + b)/2
          val old = cur._3
          val fl = fun(linspace(a, m, cells + 1))
          val fr = fun(linspace(m, b, cells + 1))
          val left = simpsonN((m-a)/cells, fl)
          val right = simpsonN((b-m)/cells, fr)
          val all = left + right
          if (abs(all - old) > tol) {
            val newIntervals = (m, b, right) :: (a, m, left) :: intervals.tail
            quadra(newIntervals, acc, level + 1)
          } else {
            quadra(intervals.tail, acc + all, level)
          }
        }
      }
    }
    val init = simpsonN((b-a)/cells, fun(linspace(a, b, cells)))
    quadra(List((a, b, init)), 0.0, 0)
  }

  /** this class is to hold the interval end points and their values a, f(a), b, f(b) */
  case class Mem(a: Double, m: Double, b: Double, fa: Double, fm: Double, fb: Double)

  def quadrature(fun: Double => Double, a: Double, b: Double,
                 maxLevel: Int = 2000, tol: Double = 1e-6): Option[Double] = {
    @tailrec
    def quadraInternal(intervals: List[Mem], acc: Double, level: Int): Option[Double] = {
      if (level > maxLevel) {
        None
      } else {
        if (intervals.isEmpty) {
          Some(acc)
        } else {
          val cur = intervals.head
          val a = cur.a
          val b = cur.b
          val ml = (a+cur.m)/2
          val fml = fun(ml)
          val mr = (cur.m+b)/2
          val fmr = fun(mr)
          val h = (b-a)/2
          val old = simpson2(h, cur.fa, cur.fm, cur.fb)
          val left = simpson2(h/2, cur.fa, fml, cur.fm)
          val right = simpson2(h/2, cur.fm, fmr, cur.fb)
          val all = left + right
          if (abs(all - old) > tol) {
            val newIntervals = Mem(cur.m, mr, b, cur.fm, fmr, b) :: Mem(a, ml, cur.m, cur.fa, fml, cur.fm) :: intervals.tail
            quadraInternal(newIntervals, acc, level + 1)
          } else {
            quadraInternal(intervals.tail, acc + all, level)
          }
        }
      }
    }
    quadraInternal(List(Mem(a, (a+b)/2, b, fun(a), fun((a+b)/2), fun(b))), 0.0, 0)
  }

  def insert(array: Array[Int], x: Int): Int = {
    @tailrec
    def rec(low: Int, up: Int): Int = {

      if (low == up) {
        low
      } else if (x >= array(up)) {
        up
      } else if (x == array((low + up)/2)) {
        (low + up)/2
      } else if (x < array((low + up)/2)) {
        rec(low, (low + up)/2 - 1)
      } else {
        rec((low + up)/2 + 1, up)
      }
    }
    rec(0, array.length - 1)
  }

}
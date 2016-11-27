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
    (vectors(*, ::) :* pow(values, 0.5)) * vectors.t
  }

  def simpson2(h: Double, fa: Double, fm: Double, fb: Double): Double = {
    (fa + 4* fm + fb) * h/3
  }
  def simpsonN(h: Double, fn: DenseVector[Double]): Double = {
    val n = fn.length
    val s = fn(0) + fn(-1) + 2.0 * sum(fn(Range(2, n, 2))) + 4.0 * sum(fn(Range(1,n,2)))
    s * h / 3
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
          val cur = intervals.head
          val a = cur._1
          val b = cur._2
          val m = (a + b)/2
          val old = cur._3
          val fl = fun(linspace(a, m, cells))
          val fr = fun(linspace(m, b, cells))
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
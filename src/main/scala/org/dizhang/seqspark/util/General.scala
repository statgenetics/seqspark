package org.dizhang.seqspark.util

import breeze.linalg.{*, CSCMatrix, DenseMatrix => DM, DenseVector, eigSym}
import breeze.numerics._

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
    require(cm.cols == dv.length, "length of the dense vector must equal columns of the matrix")
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

}

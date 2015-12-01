package org.dizhang.seqa.stat

import breeze.linalg.{DenseVector, DenseMatrix}

/**
 * Hold regression result
 */
trait TestResult {

  def estimate: Option[DenseVector[Double]]

  def stdErr: Option[DenseVector[Double]]

  def statistic: DenseVector[Double]

  def pValue: DenseVector[Double]

  override def toString = estimate match {
    case None => s",,$statistic,$pValue"
    case _ => s"$estimate,$stdErr,$statistic,$pValue"
  }

}


package org.dizhang.seqa.stat


/**
 * Hold regression result
 */
trait TestResult {

  def estimate: Option[Double]

  def stdErr: Option[Double]

  def statistic: Double

  def pValue: Double

  override def toString = estimate match {
    case None => s",,$statistic,$pValue"
    case _ => s"$estimate,$stdErr,$statistic,$pValue"
  }

}


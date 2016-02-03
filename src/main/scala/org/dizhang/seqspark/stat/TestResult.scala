package org.dizhang.seqspark.stat


/**
 * Hold regression result
 */

object TestResult {
  def apply(est: Option[Double],
            std: Option[Double],
            sta: Double,
            pVa: Double): TestResult = {
    new TestResult {
      override def estimate: Option[Double] = est

      override def stdErr: Option[Double] = std

      override def pValue: Double = pVa

      override def statistic: Double = sta
    }
  }
}

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


package org.dizhang.seqspark.stat

import breeze.linalg.{DenseVector, sum}
import LinearCombinationChiSquare._

/**
  * Linear Combination of Chi-Square distributions
  *
  */
@SerialVersionUID(7778520001L)
trait LinearCombinationChiSquare extends Serializable {
  def lambda: DenseVector[Double]
  def nonCentrality: DenseVector[Double]
  def degreeOfFreedom: DenseVector[Double]
  def cdf(cutoff: Double): CDF

  val meanLambda: Double = sum(lambda)
  val size = lambda.length

}

object LinearCombinationChiSquare {
  @SerialVersionUID(7778550101L)
  trait CDF extends Serializable {
    def pvalue: Double
    def ifault: Int
    def trace: Array[Double]
  }
}

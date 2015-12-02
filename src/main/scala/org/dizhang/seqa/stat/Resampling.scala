package org.dizhang.seqa.stat

import breeze.linalg.{DenseMatrix, shuffle, DenseVector}
import com.typesafe.config.Config
import org.dizhang.seqa.assoc.Encode
import org.dizhang.seqa.assoc.Encode.Coding
import org.dizhang.seqa.ds.Counter.CounterElementSemiGroup.PairInt
import org.dizhang.seqa.stat.ScoreTest
import org.dizhang.seqa.util.InputOutput.Var

/**
  * resampling class
  * permutation or bootstrap
  *
  */



sealed trait Resampling {
  def refStatistic: Double
  def min: Int
  def max: Int
  def binaryTrait: Boolean
  def makeNewY: DenseVector[Double]
  def makeNewCov(newY: DenseVector[Double]): Option[DenseMatrix[Double]]
  def makeNewX(newY: DenseVector[Double], newCov: Option[DenseMatrix[Double]]): Coding
  def pCount: PairInt = {
    var res = (0, 0)
    for (i <- 0 to max) {
      if (res._1 >= min)
        return res
      else {
        val newY = makeNewY
        val newCov = makeNewCov(newY)
        val newX = makeNewX(newY, newCov)
        val covBeta = newCov match {
          case None => None
          case Some(c) => Some(
            if (binaryTrait)
              new LogisticRegression(newY, c).estimates
            else
              new LinearRegression(newY, c).estimates)
        }
        val statistic = ScoreTest(binaryTrait, newY, newX, newCov, covBeta).summary.statistic
        res = PairInt.op(res, if (statistic >= refStatistic) (1, 1) else (0, 1))
      }
    }
    res
  }
}

sealed trait XFixed extends Resampling {
  def x: Coding
  def makeNewX(newY: DenseVector[Double], newCov: Option[DenseMatrix[Double]]) = x
}

sealed trait XRecoding extends Resampling {
  def x: Iterable[Var]
  def controls: Option[Array[Boolean]]
  def config: Config
  def makeNewX(newY: DenseVector[Double], newCov: Option[DenseMatrix[Double]]): Coding =
    Encode(x, newY.length, controls, Some(newY), newCov, config).getCoding.get
}

sealed trait YPermutation extends Resampling {
  def y: DenseVector[Double]
  def makeNewY: DenseVector[Double] = shuffle(y)
}

case class XFixedYPermutation(refStatistic: Double,
                             binaryTrait: Boolean,
                             x: Coding,
                             y: DenseVector[Double],
                             min: Int,
                             max: Int) extends Resampling with YPermutation with XFixed

case class XRecodingYPermutation(refStatistic: Double,
                               min: Int,
                               max: Int,
                               binaryTrait: Boolean,
                               x: Iterable[Var],
                               y: DenseVector[Double],
                               controls: Option[Array[Boolean]],
                               config: Config) extends Resampling with YPermutation with XRecoding

case class QuantPermutation(refStatistic: Double,
                            min: Int,
                            max: Int,
                            x: )
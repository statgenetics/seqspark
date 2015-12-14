package org.dizhang.seqa.stat

import breeze.linalg.{DenseMatrix, shuffle, DenseVector}
import breeze.stats.distributions.Bernoulli
import com.typesafe.config.Config
import org.dizhang.seqa.assoc.Encode
import org.dizhang.seqa.assoc.Encode.Coding
import org.dizhang.seqa.ds.Counter.CounterElementSemiGroup.PairInt
import org.dizhang.seqa.util.InputOutput.Var

/**
  * resampling class
  * genotype never change, or re-encoded every time
  * y permutation, residue permutation, or bootstrap
  *
  */

object Resampling {

  def apply(ref: Double,
            min: Int,
            max: Int,
            x: Encode,
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            e: Option[DenseVector[Double]],
            controls: Option[Array[Boolean]],
            config: Option[Config],
            bt: Boolean): Resampling = {
    (config, controls) match {
      case (None, None) =>
        apply(ref, min, max, x.getCoding.get, y, cov, e, bt)
      case (Some(cnf), _) =>
        apply(ref, min, max, x.vars, y, cov, e, controls, cnf, bt)
    }
  }


  def apply(ref: Double,
            min: Int,
            max: Int,
            x: Coding,
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            e: Option[DenseVector[Double]],
            bt: Boolean): Resampling = {
    (bt, cov) match {
      case (_, None) => new XFixedYPermutation(ref, min, max, x, y, bt)
      case (true, Some(c)) =>
        require(e.isDefined, "estimates have to be defined")
        new XFixedYBootstrap(ref, min, max, x, y, Some(c), e.get)
      case (false, Some(c)) =>
        require(e.isDefined, "estimates have to be defined")
        new XFixedRPermutation(ref, min, max, x, y, Some(c), e.get)
    }
  }

  def apply(ref: Double,
            min: Int,
            max: Int,
            x: Iterable[Var],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            e: Option[DenseVector[Double]],
            controls: Option[Array[Boolean]],
            config: Config,
            bt: Boolean): Resampling = {
    (bt, cov) match {
      case (_, None) => new XRecodingYPermutation(ref, min, max, x, y, bt, controls, config)
      case (true, Some(c)) =>
        require(e.isDefined, "estimates have to be defined")
        new XRecodingYBootstrap(ref, min, max, x, y, Some(c), e.get, controls, config)
      case (false, Some(c)) =>
        require(e.isDefined, "estimates have to be defined")
        new XRecodingRPermutation(ref, min, max, x, y, Some(c), e.get, controls, config)
    }
  }


}

sealed trait Resampling extends HypoTest {
  def refStatistic: Double
  def min: Int
  def max: Int
  def binaryTrait: Boolean
  def cov: Option[DenseMatrix[Double]]
  def makeNewY: DenseVector[Double]
  def makeNewX(newY: DenseVector[Double]): Coding
  /** newE is new estimate of Y,
    * based on the regression model: y ~ cov */
  def makeNewE(newY: DenseVector[Double]): Option[DenseVector[Double]]
  def pCount: PairInt = {
    var res = (0, 0)
    for (i <- 0 to max) {
      if (res._1 >= min)
        return res
      else {
        val newY = makeNewY
        val newX = makeNewX(newY)
        val newEstimates = makeNewE(newY)
        val statistic = ScoreTest(binaryTrait, newY, newX, cov, newEstimates).summary.statistic
        res = PairInt.op(res, if (statistic >= refStatistic) (1, 1) else (0, 1))
      }
    }
    res
  }
}

sealed trait XFixed extends Resampling {
  def x: Coding
  def makeNewX(newY: DenseVector[Double]) = x
}

sealed trait XRecoding extends Resampling {
  def x: Iterable[Var]
  def controls: Option[Array[Boolean]]
  def config: Config
  def makeNewX(newY: DenseVector[Double]): Coding =
    Encode(x, newY.length, controls, Some(newY), cov, config).getCoding.get
}

sealed trait YPermutation extends Resampling {
  def y: DenseVector[Double]
  /** No covariates, no need to generate newE */
  def cov = None
  def makeNewE(newY: DenseVector[Double]) = None
  def makeNewY: DenseVector[Double] = shuffle(y)
}

sealed trait RPermutation extends Resampling {
  /**
    * R for Residue, this case class is for residue permutation
    */
  def y: DenseVector[Double]
  def cov: Some[DenseMatrix[Double]]
  def estimates: DenseVector[Double]
  def binaryTrait = false
  def makeNewY = estimates + shuffle(y - estimates)
  def makeNewE(newY: DenseVector[Double]) = {
    Some(new LinearRegression(newY, cov.get).estimates)
  }
}

sealed trait YBootstrap extends Resampling {
  def y: DenseVector[Double]
  def cov: Some[DenseMatrix[Double]]
  def estimates: DenseVector[Double]
  def binaryTrait = true
  def makeNewY = estimates.map(p => if (new Bernoulli(p).draw()) 1.0 else 0.0)
  def makeNewE(newY: DenseVector[Double]) = {
    Some(new LogisticRegression(newY, cov.get).estimates)
  }
}

case class XFixedYPermutation(refStatistic: Double,
                              min: Int,
                              max: Int,
                              x: Coding,
                              y: DenseVector[Double],
                              binaryTrait: Boolean) extends Resampling with YPermutation with XFixed

case class XRecodingYPermutation(refStatistic: Double,
                                 min: Int,
                                 max: Int,
                                 x: Iterable[Var],
                                 y: DenseVector[Double],
                                 binaryTrait: Boolean,
                                 controls: Option[Array[Boolean]],
                                 config: Config) extends Resampling with YPermutation with XRecoding

case class XFixedRPermutation(refStatistic: Double,
                              min: Int,
                              max: Int,
                              x: Coding,
                              y: DenseVector[Double],
                              cov: Some[DenseMatrix[Double]],
                              estimates: DenseVector[Double]) extends Resampling with RPermutation with XFixed

case class XRecodingRPermutation(refStatistic: Double,
                                 min: Int,
                                 max: Int,
                                 x: Iterable[Var],
                                 y: DenseVector[Double],
                                 cov: Some[DenseMatrix[Double]],
                                 estimates: DenseVector[Double],
                                 controls: Option[Array[Boolean]],
                                 config: Config) extends Resampling with RPermutation with XRecoding

case class XFixedYBootstrap(refStatistic: Double,
                            min: Int,
                            max: Int,
                            x: Coding,
                            y: DenseVector[Double],
                            cov: Some[DenseMatrix[Double]],
                            estimates: DenseVector[Double]) extends Resampling with YBootstrap with XFixed

case class XRecodingYBootstrap(refStatistic: Double,
                               min: Int,
                               max: Int,
                               x: Iterable[Var],
                               y: DenseVector[Double],
                               cov: Some[DenseMatrix[Double]],
                               estimates: DenseVector[Double],
                               controls: Option[Array[Boolean]],
                               config: Config) extends Resampling with YBootstrap with XRecoding
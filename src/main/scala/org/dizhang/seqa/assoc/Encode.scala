package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.pow
import com.typesafe.config.Config
import org.dizhang.seqa.ds.Counter
import org.dizhang.seqa.stat.{LinearRegression, LogisticRegression}
import org.dizhang.seqa.util.Constant
import org.dizhang.seqa.util.Constant.UnPhased
import org.dizhang.seqa.util.Constant.UnPhased.Bt
import org.dizhang.seqa.util.InputOutput.Var
import org.dizhang.seqa.worker.GenotypeLevelQC.{getMaf, makeMaf}
import Encode._

/**
  * How to code variants in one group (gene)
  */
object Encode {
  /** constants values of the pathes and values of
    * the method config object*/
  val CPMethod = Constant.ConfigPath.Association.SomeMethod
  val CVMethod = Constant.ConfigValue.Association.SomeMethod

  trait Coding
  case class Fixed(coding: DenseVector[Double]) extends Coding
  case class VT(coding: DenseMatrix[Double]) extends Coding

  def cmcMakeNaAdjust(x: Byte, maf: Double): Double =
    x match {
      case Bt.ref => 0.0
      case Bt.het1 => 1.0
      case Bt.het2 => 1.0
      case Bt.mut => 1.0
      case _ => 1.0 - math.pow(1.0 - maf, 2)}

  object CmcAddNaAdjust extends Counter.CounterElementSemiGroup[Double] {
    def zero = 0.0

    def pow(x: Double, i: Int) =
      x match {
        case 1.0 => 1.0
        case 0.0 => 0.0
        case _ => 1.0 - math.pow(1.0 - x, i)}

    def op(a: Double, b: Double) =
      if (a == 1.0 || b == 1.0)
        1.0
      else
        1.0 - (1.0 - a) * (1.0 - b)}

  def brvMakeNaAdjust(x: Byte, maf: Double): Double =
    x match {
      case Bt.ref => 0.0
      case Bt.het1 => 1.0
      case Bt.het2 => 1.0
      case Bt.mut => 2.0
      case _ => 2 * maf
    }

  val BrvAddNaAdjust = Counter.CounterElementSemiGroup.AtomDouble

  /** The authors just said the sample size needed is very large, but not how large
    * this function is full of magic numbers !!!
    * */

  def erecDelta(n: Int): Double = {
    if (n < 2000)
      1.0
    else if (n > 100000)
      0.1
    else
      0.1 + 0.9 * (n - 2000) / 98000
  }

  def apply(vars: Iterable[Var],
            sampleSize: Int,
            controls: Option[Array[Boolean]] = None,
            y: Option[DenseVector[Double]],
            cov: Option[DenseMatrix[Double]],
            config: Config): Encode = {
    val codingScheme = config.getString(CPMethod.coding)
    val mafSource = config.getString(CPMethod.Maf.source)
    val weightMethod = config.getString(CPMethod.weight)
    (codingScheme, mafSource, weightMethod) match {
      case (CVMethod.Coding.brv, CVMethod.Maf.Source.controls, CVMethod.Weight.erec) =>
        apply(vars, sampleSize, controls.get, y.get, cov, config)
      case (CVMethod.Coding.brv, _, CVMethod.Weight.erec) =>
        apply(vars, sampleSize, y.get, cov, config)
      case (_, CVMethod.Maf.Source.controls, _) =>
        apply(vars, sampleSize, controls.get, config)
      case (_, _, _) =>
        apply(vars, sampleSize, config)
    }
  }


  def apply(vars: Iterable[Var], sampleSize: Int, config: Config): Encode = {
    config.getString(CPMethod.coding) match {
      case CVMethod.Coding.single => DefaultSingle(vars, sampleSize, config)
      case CVMethod.Coding.cmc => DefaultCMC(vars, sampleSize, config)
      case CVMethod.Coding.brv => SimpleBRV(vars, sampleSize, config)
      case _ => DefaultCMC(vars, sampleSize, config)
    }
  }

  def apply(vars: Iterable[Var], sampleSize: Int, controls: Array[Boolean], config: Config): Encode = {
    config.getString(CPMethod.coding) match {
      case CVMethod.Coding.single => ControlsMafSingle(vars, sampleSize, controls, config)
      case CVMethod.Coding.cmc => ControlsMafCMC(vars, sampleSize, controls, config)
      case CVMethod.Coding.brv => ControlsMafSimpleBRV(vars, sampleSize, controls, config)
      case _ => ControlsMafCMC(vars, sampleSize, controls, config)
    }
  }

  def apply(vars: Iterable[Var],
            sampleSize: Int,
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: Config): Encode = {
    ErecBRV(vars, sampleSize, y, cov, config)
  }

  def apply(vars: Iterable[Var],
            sampleSize: Int,
            controls: Array[Boolean],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: Config): Encode = {
    ControlsMafErecBRV(vars, sampleSize, controls, y, cov, config)
  }
}

sealed trait Encode {
  def vars: Iterable[Var]
  def maf: Array[Double]
  def sampleSize: Int
  def config: Config
  lazy val fixedCutoff: Double = config.getDouble(CPMethod.Maf.cutoff)
  def thresholds: Option[Array[Double]] = {
    val n = sampleSize
    /** this is to make sure 90% call rate sites is considered the same with the 100% cr ones
      * (with 1 - 4 minor alleles. anyway 5 is indistinguishable.
      * Because 5.0/0.9n - 5.0/n == 5.0/n - 4.0/0.9n */
    val tol = 4.0/(9 * n)
    val sortedMaf = maf.filter(m => m < fixedCutoff).sorted
    if (sortedMaf.isEmpty)
      None
    else
      Some(sortedMaf.map(c => Array(c)).reduce((a, b) =>
        if (a(-1) + tol >= b(0))
          a.slice(0, a.length - 1) ++ b
        else
          a ++ b))
  }
  def getFixed(cutoff: Double = fixedCutoff): Option[Fixed]
  def getVT: Option[VT] = {
    thresholds match {
      case None => None
      case Some(th) =>
        Some(VT(DenseVector.horzcat(th.map(c => this.getFixed(c).get.coding): _*)))
    }
  }
  def getCoding: Option[Coding] = {
    if (config.getBoolean(CPMethod.Maf.fixed))
      this.getFixed()
    else
      this.getVT
  }
  def isDefined: Boolean
}

sealed trait Single extends Encode {
  def isDefined = maf.exists(_ >= fixedCutoff)
  def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {
    val tmp = vars.zip(maf).filter(p => p._2 >= cutoff)
    if (! this.isDefined)
      None
    else {
      val res = tmp.reduce((a, b) => a)
      Some(Fixed(DenseVector(res._1.map{
        case UnPhased.Bt.ref => 0.0
        case UnPhased.Bt.het1 => 1.0
        case UnPhased.Bt.het2 => 1.0
        case UnPhased.Bt.mut => 2.0
        case _ => 2.0 * res._2
      }.toArray)))
    }
  }
}

sealed trait CMC extends Encode {
  def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {
    if (! this.isDefined)
      None
    else
      Some(Fixed(vars.zip(maf).filter(v => v._2 < cutoff).map(v =>
        v._1.toCounter(cmcMakeNaAdjust(_, v._2), 0.0)
      ).reduce((a, b) => a.++(b)(CmcAddNaAdjust)).toDenseVector(x => x)))
  }
  def isDefined = maf.exists(_ < fixedCutoff)
}

sealed trait BRV extends Encode {
  def isDefined = maf.exists(_ < fixedCutoff)
  def getGenotype(cutoff: Double = fixedCutoff): DenseMatrix[Double] = {
    val tmp = vars.zip(maf).filter(v => v._2 < cutoff).map(v =>
      v._1.toCounter(brvMakeNaAdjust(_, v._2), 0.0).toArray).toArray
    DenseMatrix(tmp: _*).t}

  def weight(cutoff: Double = fixedCutoff): Option[DenseVector[Double]]

  def getFixed(cutoff: Double = fixedCutoff): Option[DenseVector[Double]] = {
    val genotype = this.getGenotype(cutoff)
    if (! this.isDefined)
      None
    else
      weight(cutoff) match {
        case None => Some(genotype * DenseVector.fill(genotype.size)(1.0))
        case Some(w) => Some(genotype * w)}}
}

sealed trait PooledOrAnnotationMaf extends Encode {
  def maf = {
    config.getString(CPMethod.Maf.source) match {
      case CVMethod.Maf.Source.annotation =>
        vars.map(v => v.parseInfo(Constant.Variant.InfoKey.maf).toDouble).toArray
      case CVMethod.Maf.Source.pooled =>
        vars.map (v => getMaf (v.toCounter (makeMaf, (0, 2) ).reduce) ).toArray
      case _ => vars.map (v => getMaf(v.toCounter(makeMaf, (0, 2)).reduce)).toArray
    }
  }
}

sealed trait ControlsMaf extends Encode {
  def controls: Array[Boolean]
  def maf = vars.map(v => getMaf(v.select(controls).toCounter(makeMaf, (0, 2)).reduce)).toArray
}

sealed trait SimpleWeight extends BRV {
  def weight(cutoff: Double): Option[DenseVector[Double]] = {
    config.getString(CPMethod.weight) match {
      case CVMethod.Weight.equal => None
      case CVMethod.Weight.annotation =>
        Some(DenseVector(vars.map(v => v.parseInfo(Constant.Variant.InfoKey.weight).toDouble).zip(maf)
          .filter(v => v._2 < cutoff).map(_._1).toArray))
      case CVMethod.Weight.wss =>
        val mafDV = DenseVector(this.maf.filter(m => m < cutoff))
        Some(pow(mafDV :* mafDV.map(1.0 - _), -0.5))
      case _ => None
    }
  }
}

sealed trait LearnedWeight extends BRV {
  def y: DenseVector[Double]
  def cov: Option[DenseMatrix[Double]]
  def weight(cutoff: Double) = {
    val n = vars.size
    val combined = cov match {
      case None => getGenotype(cutoff)
      case Some(c) => DenseMatrix.horzcat(getGenotype(cutoff), c)
    }
    val beta =
      if (y.toArray.count(_ == 0.0) + y.toArray.count(_ == 1.0) == y.length) {
        val model = new LogisticRegression(y, combined)
        model.estimates.map(x => x + erecDelta(y.length))
      } else {
        val model = new LinearRegression(y, combined)
        model.estimates.map(x => x + 2 * erecDelta(y.length))
      }
    Some(beta(1 to n))
  }
}

case class DefaultSingle(vars: Iterable[Var],
                         sampleSize: Int,
                         config: Config) extends Encode with Single with PooledOrAnnotationMaf

case class ControlsMafSingle(vars: Iterable[Var],
                             sampleSize: Int,
                             controls: Array[Boolean],
                             config: Config) extends Encode with Single with ControlsMaf

case class DefaultCMC(vars: Iterable[Var],
                      sampleSize: Int,
                      config: Config) extends Encode with CMC with PooledOrAnnotationMaf

case class ControlsMafCMC(vars: Iterable[Var],
                          sampleSize: Int,
                          controls: Array[Boolean],
                          config: Config) extends Encode with CMC with ControlsMaf

case class SimpleBRV(vars: Iterable[Var],
                     sampleSize: Int,
                     config: Config) extends Encode with BRV with PooledOrAnnotationMaf with SimpleWeight


case class ControlsMafSimpleBRV(vars: Iterable[Var],
                          sampleSize: Int,
                          controls: Array[Boolean],
                          config: Config) extends Encode with BRV with ControlsMaf with SimpleWeight

case class ErecBRV(vars: Iterable[Var],
                   sampleSize: Int,
                   y: DenseVector[Double],
                   cov: Option[DenseMatrix[Double]],
                   config: Config) extends Encode with BRV with PooledOrAnnotationMaf with LearnedWeight

case class ControlsMafErecBRV(vars: Iterable[Var],
                              sampleSize: Int,
                              controls: Array[Boolean],
                              y: DenseVector[Double],
                              cov: Option[DenseMatrix[Double]],
                              config: Config) extends Encode with BRV with ControlsMaf with LearnedWeight

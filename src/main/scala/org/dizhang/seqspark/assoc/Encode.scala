package org.dizhang.seqspark.assoc

import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, SparseVector}
import breeze.numerics.{exp, lbeta, pow}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.stat.{LinearRegression, LogisticRegression}
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.UserConfig._
import Encode._
import org.dizhang.seqspark.util.Constant.Variant.InfoKey
import org.dizhang.seqspark.geno.SimpleVCF.SimpleGenotype
import org.dizhang.seqspark.geno.ImputedVCF.ImputedGenotype

/**
  * How to code variants, either in one group (gene) or in a region
  * This class only applies to burden test
  * For meta and Skat, use the raw encoding
  *
  */

object Encode {

  type Imputed = (Double, Double, Double)
  type Var = Variant[_]

  def makeMaf(x: _): (Double, Double) = {
    x match {
      case g: Byte => g.maf
      case g: Imputed => g.maf
      case _ => (-9.0, 1.0)
    }
  }

  def cmcMakeNaAdjust(x: _, maf: Double): Double = {
    x match {
      case g: Byte => g.toCMC(maf)
      case g: Imputed => g.toCMC(maf)
      case _ => -9.0
    }
  }

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

  def brvMakeNaAdjust(x: _, maf: Double): Double = {
    x match {
      case g: Byte => g.toBRV(maf)
      case g: Imputed => g.toBRV(maf)
      case _ => -9.0
    }
  }

  val BrvAddNaAdjust = Counter.CounterElementSemiGroup.AtomDouble

  def erecDelta(n: Int): Double = {
    /** The authors just said the sample size needed is very large, but not how large
      * this function is full of magic numbers !!!
      * */
    if (n < 2000)
      1.0
    else if (n > 100000)
      0.1
    else
      0.1 + 0.9 * (n - 2000) / 98000
  }



  def apply(vars: Iterable[Var],
            controls: Option[Array[Boolean]] = None,
            y: Option[DenseVector[Double]],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode = {
    val codingScheme = config.`type`
    val mafSource = config.maf.getString("source")
    val weightMethod = config.weight
    (codingScheme, mafSource, weightMethod) match {
      case (MethodType.brv, "controls", WeightMethod.erec) =>
        apply(vars, controls.get, y.get, cov, config)
      case (MethodType.brv, _, WeightMethod.erec) =>
        apply(vars, y.get, cov, config)
      case (_, "controls", _) =>
        apply(vars, controls.get, config)
      case (_, _, _) =>
        apply(vars, config)
    }
  }

  def apply(varsIter: Iterable[Var], config: MethodConfig): Encode = {
    val vars = varsIter.toArray
    config.`type` match {
      case MethodType.single => DefaultSingle(vars, config)
      case MethodType.cmc => DefaultCMC(vars, config)
      case MethodType.brv => SimpleBRV(vars, config)
      case MethodType.skat => SimpleBRV(vars, config)
      case MethodType.meta => DefaultRaw(vars, config)
      case _ => DefaultCMC(vars, config)
    }
  }

  def apply(varsIter: Iterable[Var], controls: Array[Boolean], config: MethodConfig): Encode = {
    val vars = varsIter.toArray
    config.`type` match {
      case MethodType.single => ControlsMafSingle(vars, controls, config)
      case MethodType.cmc => ControlsMafCMC(vars, controls, config)
      case MethodType.brv => ControlsMafSimpleBRV(vars, controls, config)
      case MethodType.skat => ControlsMafSimpleBRV(vars, controls, config)
      case MethodType.meta => ControlsMafRaw(vars, controls, config)
      case _ => ControlsMafCMC(vars, controls, config)
    }
  }

  def apply(varsIter: Iterable[Var],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode = {
    ErecBRV(varsIter.toArray, y, cov, config)
  }

  def apply(varsIter: Iterable[Var],
            controls: Array[Boolean],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode = {
    val vars = varsIter.toArray
    ControlsMafErecBRV(vars, controls, y, cov, config)
  }

  trait Coding
  case class Rare(coding: CSCMatrix[Double], vars: Array[Variation]) extends Coding
  case class Common(coding: DenseMatrix[Double], vars: Array[Variation]) extends Coding
  case class Fixed(coding: SparseVector[Double], vars: Array[Variation]) extends Coding
  case class VT(coding: CSCMatrix[Double], vars: Array[Variation]) extends Coding

  sealed trait Raw extends Encode {
    def isDefined = maf.exists(_ > 0.0)
    def getFixed(cutoff: Double = fixedCutoff) = None
    override def getCoding = None
  }

  sealed trait Single extends Encode {
    override def getRare(cutoff: Double) = None
    def weight(cutoff: Double) = DenseVector(1.0)
    def isDefined = maf.exists(_ >= fixedCutoff)
    def getFixed(cutoff: Double = fixedCutoff) = None
  }

  sealed trait CMC extends Encode {
    def weight(cutoff: Double) = DenseVector[Double]()
    def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {
      definedIndices(_ < cutoff).map{idx =>
        val sv = idx.map{i =>
          vars(i).toCounter(cmcMakeNaAdjust(_, maf(i)), 0.0)
        }.reduce((a, b) => a.++(b)(CmcAddNaAdjust)).toSparseVector(x => x)
        val variations = idx.map{ i =>
          val mc = mafCount(i)
          val res = vars(i).toVariation()
          res.addInfo(InfoKey.maf, s"${mc._1},${mc._2}")
        }
        Fixed(sv, variations)
      }
    }
    def isDefined = maf.exists(_ < fixedCutoff)
  }

  sealed trait BRV extends Encode {
    def isDefined = maf.exists(_ < fixedCutoff)

    def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {
      val w = weight(cutoff)
      definedIndices(_ < cutoff).map{idx =>
        val sv = idx.filter(maf(_) < cutoff).map { i =>
          vars(i).toCounter(brvMakeNaAdjust(_, maf(i)) * w(i), 0.0)
        }.reduce((a, b) => a.++(b)(BrvAddNaAdjust)).toSparseVector(x => x)
        val variations = idx.map{ i =>
          val mc = mafCount(i)
          val res = vars(i).toVariation()
          res.addInfo(InfoKey.maf, s"${mc._1},${mc._2}")
        }
        Fixed(sv, variations)
      }
    }
  }

  sealed trait PooledOrAnnotationMaf extends Encode {
    lazy val mafCount = vars.map(v => v.toCounter(makeMaf, (0.0, 2.0)).reduce)

    lazy val maf = {
      config.maf.getString("source") match {
        case "pooled" => mafCount.map(_.ratio)
        case key =>
          vars.map(v => v.parseInfo(key).toDouble)
      }
    }
  }

  sealed trait ControlsMaf extends Encode {
    def controls: Array[Boolean]
    lazy val mafCount = vars.map(v => v.select(controls).toCounter(makeMaf, (0.0, 2.0)).reduce)
    lazy val maf = mafCount.map(_.ratio)
  }

  sealed trait SimpleWeight extends BRV {
    def weight(cutoff: Double): DenseVector[Double] = {
      val mafDV = DenseVector(this.maf.filter(m => m < cutoff))
      config.weight match {
        case WeightMethod.annotation =>
          DenseVector(vars.map(v => v.parseInfo(Constant.Variant.InfoKey.weight).toDouble).zip(maf)
            .filter(v => v._2 < cutoff).map(_._1))
        case WeightMethod.wss =>
          pow(mafDV :* mafDV.map(1.0 - _), -0.5)
        case WeightMethod.skat =>
          mafDV.map(m => 1.0/exp(lbeta(1, 25)) * pow(1 - m, 24.0))
        case _ => DenseVector.fill[Double](mafDV.length)(1.0)
      }
    }
  }

  sealed trait LearnedWeight extends BRV {
    def y: DenseVector[Double]
    def cov: Option[DenseMatrix[Double]]
    def weight(cutoff: Double = fixedCutoff) = {
      val geno = getRare(cutoff).get.coding.toDense
      val n = geno.cols
      val combined = cov match {
        case None => geno
        case Some(c) => DenseMatrix.horzcat(geno, c)
      }
      val beta =
        if (y.toArray.count(_ == 0.0) + y.toArray.count(_ == 1.0) == y.length) {
          val model = LogisticRegression(y, combined)
          model.estimates.map(x => x + erecDelta(y.length))
        } else {
          val model = LinearRegression(y, combined)
          model.estimates.map(x => x + 2 * erecDelta(y.length))
        }
      beta(1 to n)
    }
  }

  case class DefaultRaw(vars: Array[Var],
                        config: MethodConfig) extends Encode with Raw with PooledOrAnnotationMaf with SimpleWeight

  case class ControlsMafRaw(vars: Array[Var],
                            controls: Array[Boolean],
                            config: MethodConfig) extends Encode with Raw with ControlsMaf with SimpleWeight

  case class DefaultSingle(vars: Array[Var],
                           config: MethodConfig) extends Encode with Single with PooledOrAnnotationMaf

  case class ControlsMafSingle(vars: Array[Var],
                               controls: Array[Boolean],
                               config: MethodConfig) extends Encode with Single with ControlsMaf

  case class DefaultCMC(vars: Array[Var],
                        config: MethodConfig) extends Encode with CMC with PooledOrAnnotationMaf

  case class ControlsMafCMC(vars: Array[Var],
                            controls: Array[Boolean],
                            config: MethodConfig) extends Encode with CMC with ControlsMaf

  case class SimpleBRV(vars: Array[Var],
                       config: MethodConfig) extends Encode with BRV with PooledOrAnnotationMaf with SimpleWeight


  case class ControlsMafSimpleBRV(vars: Array[Var],
                                  controls: Array[Boolean],
                                  config: MethodConfig) extends Encode with BRV with ControlsMaf with SimpleWeight

  case class ErecBRV(vars: Array[Var],
                     y: DenseVector[Double],
                     cov: Option[DenseMatrix[Double]],
                     config: MethodConfig) extends Encode with BRV with PooledOrAnnotationMaf with LearnedWeight

  case class ControlsMafErecBRV(vars: Array[Var],
                                controls: Array[Boolean],
                                y: DenseVector[Double],
                                cov: Option[DenseMatrix[Double]],
                                config: MethodConfig) extends Encode with BRV with ControlsMaf with LearnedWeight


}

@SerialVersionUID(7727390001L)
sealed trait Encode extends Serializable {
  def vars: Array[Var]
  def mafCount: Array[(Double, Double)]
  def maf: Array[Double]
  def sampleSize: Int = if (vars.isEmpty) 0 else vars.head.length
  def config: MethodConfig
  lazy val fixedCutoff: Double = config.maf.getDouble("cutoff")
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
  def weight(cutoff: Double = fixedCutoff): DenseVector[Double]
  def getFixed(cutoff: Double = fixedCutoff): Option[Encode.Fixed]

  def getVT: Option[Encode.VT] = {
    thresholds.map{th =>
      val cm = SparseVector.horzcat(th.map(c => this.getFixed(c).get.coding): _*)
      val variations = vars.map(_.toVariation()).zip(mafCount).filter(p => p._2.ratio <= th.max).map{
        case (v, mc) =>
          v.addInfo(InfoKey.maf, s"${mc._1},${mc._2}")
      }
      Encode.VT(cm, variations)
    }
  }

  def getCoding: Option[Encode.Coding] = {
    if (config.maf.getBoolean("fixed"))
      this.getFixed()
    else
      this.getVT
  }
  def isDefined: Boolean
  def getCommon(cutoff: Double = fixedCutoff): Option[Common] = {
    if (maf.forall(_ < cutoff)) {
      None
    } else {
      val res = DenseVector.horzcat(vars.zip(maf).filter(v => v._2 >= cutoff).map {
        case (v, m) => DenseVector(v.toArray.map(brvMakeNaAdjust(_, m)): _*)
      }: _*)
      val info = vars.zip(mafCount).filter(v => v._2.ratio >= cutoff).map{
        case (v, m) =>
          v.toVariation().addInfo(Constant.Variant.InfoKey.maf, s"${m._1},${m._2}")
      }
      Some(Common(res, info))
    }
  }
  def definedIndices(p: Double => Boolean): Option[Array[Int]] = {
    val definedMaf = maf.zipWithIndex.filter(x => p(x._1))
    if (definedMaf.isEmpty) {
      None
    } else {
      Some(definedMaf.map(_._2))
    }
  }

  def getRare(cutoff: Double = fixedCutoff): Option[Encode.Rare] = {
    if (maf.forall(_ >= cutoff)) {
      val builder = new CSCMatrix.Builder[Double](sampleSize, maf.count(_ < cutoff))
      val cnt = vars.zip(maf).filter(v => v._2 < cutoff).map{ v =>
        v._1.toCounter(brvMakeNaAdjust(_, v._2), 0.0).toMap}
      cnt.zipWithIndex.foreach{ case (elems, col) =>
          elems.foreach{case (row, v) => builder.add(row, col, v)}}
      val info = vars.zip(mafCount).filter(v => v._2.ratio >= cutoff).map{
        case (v, m) =>
          v.toVariation().addInfo(Constant.Variant.InfoKey.maf, s"${m._1},${m._2}")
      }
      Some(Rare(builder.result, info))
    } else {
      None
    }
  }
}


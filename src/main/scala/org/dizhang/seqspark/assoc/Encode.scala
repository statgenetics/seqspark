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


/**
  * How to code variants, either in one group (gene) or in a region
  * This class only applies to burden test
  * For meta and Skat, use the raw encoding
  *
  */

object Encode {

  type Imputed = (Double, Double, Double)

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
        1.0 - (1.0 - a) * (1.0 - b)
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

  def apply[A: Genotype](vars: Iterable[Variant[A]],
            controls: Option[Array[Boolean]] = None,
            y: Option[DenseVector[Double]],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode[A] = {
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

  def apply[A: Genotype](varsIter: Iterable[Variant[A]], config: MethodConfig): Encode[A]= {
    val vars = varsIter.toArray
    config.`type` match {
      case MethodType.snv => DefaultSingle(vars, config)
      case MethodType.cmc => DefaultCMC(vars, config)
      case MethodType.brv => SimpleBRV(vars, config)
      case MethodType.skat => SimpleBRV(vars, config)
      case MethodType.meta => DefaultRaw(vars, config)
      case _ => DefaultCMC(vars, config)
    }
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]], controls: Array[Boolean], config: MethodConfig): Encode[A]= {
    val vars = varsIter.toArray
    config.`type` match {
      case MethodType.snv => ControlsMafSingle(vars, controls, config)
      case MethodType.cmc => ControlsMafCMC(vars, controls, config)
      case MethodType.brv => ControlsMafSimpleBRV(vars, controls, config)
      case MethodType.skat => ControlsMafSimpleBRV(vars, controls, config)
      case MethodType.meta => ControlsMafRaw(vars, controls, config)
      case _ => ControlsMafCMC(vars, controls, config)
    }
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode[A] = {
    ErecBRV(varsIter.toArray, y, cov, config)
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
            controls: Array[Boolean],
            y: DenseVector[Double],
            cov: Option[DenseMatrix[Double]],
            config: MethodConfig): Encode[A] = {
    val vars = varsIter.toArray
    ControlsMafErecBRV(vars, controls, y, cov, config)
  }

  trait Coding
  case class Rare(coding: CSCMatrix[Double], vars: Array[Variation]) extends Coding
  case class Common(coding: DenseMatrix[Double], vars: Array[Variation]) extends Coding
  case class Fixed(coding: SparseVector[Double], vars: Array[Variation]) extends Coding
  case class VT(coding: CSCMatrix[Double], vars: Array[Variation]) extends Coding

  sealed trait Raw[A] extends Encode[A] {
    def isDefined = maf.exists(m => m > 0.0 && m < 1.0)
    def getFixed(cutoff: Double = fixedCutoff) = None
    override def getCoding = None
    def weight = DenseVector.fill(1)(0.0)
  }

  sealed trait Single[A] extends Encode[A] {
    override def getRare(cutoff: Double) = None
    def weight = DenseVector(1.0)
    def isDefined = maf.exists(m => m >= fixedCutoff && m <= (1 - fixedCutoff))
    def getFixed(cutoff: Double = fixedCutoff) = None
  }

  sealed trait CMC[A] extends Encode[A] {
    def weight = DenseVector[Double]()
    def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {

      definedIndices(m => m < cutoff || m > (1 - cutoff)).map{idx =>
        val sv = idx.map{i =>
          vars(i).toCounter(genotype.toCMC(_, maf(i)), 0.0)
        }.reduce((a, b) => a.++(b)(CmcAddNaAdjust)).toSparseVector(x => x)
        val variations = idx.map{ i =>
          val mc = mafCount(i)
          val res = vars(i).toVariation()
          res.addInfo(InfoKey.maf, s"${mc._1},${mc._2}")
        }
        Fixed(sv, variations)
      }
    }
    def isDefined = maf.exists(m => m < fixedCutoff || m > (1 - fixedCutoff))
  }

  sealed trait BRV[A] extends Encode[A] {
    def isDefined = maf.exists(m => m < fixedCutoff || m > (1 - fixedCutoff))

    def getFixed(cutoff: Double = fixedCutoff): Option[Fixed] = {
      val w = weight
      definedIndices(m => m < cutoff || m > (1 - cutoff)).map{idx =>
        val sv = idx.map {i =>
          //println(s"i:$i vars len: ${vars.length} maf len: ${maf.length} weight len: ${w.length}")
          vars(i).toCounter(genotype.toBRV(_, maf(i)) * w(i), 0.0)
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

  sealed trait PooledOrAnnotationMaf[A] extends Encode[A] {
    lazy val mafCount = vars.map(v => v.toCounter(genotype.toAAF, (0.0, 2.0)).reduce)

    lazy val maf = {
      config.maf.getString("source") match {
        case "pooled" => mafCount.map(_.ratio)
        case key =>
          vars.map(v => v.parseInfo(key).toDouble)
      }
    }
  }

  sealed trait ControlsMaf[A] extends Encode[A] {
    def controls: Array[Boolean]
    lazy val mafCount = vars.map(v => v.select(controls).toCounter(genotype.toAAF, (0.0, 2.0)).reduce)
    lazy val maf = mafCount.map(_.ratio)
  }

  sealed trait SimpleWeight[A] extends BRV[A] {
    def weight: DenseVector[Double] = {
      val mafDV: DenseVector[Double] = DenseVector(maf)
      config.weight match {
        case WeightMethod.annotation =>
          DenseVector(vars.map(v => v.parseInfo(Constant.Variant.InfoKey.weight).toDouble))
        case WeightMethod.wss =>
          pow(mafDV :* mafDV.map(1.0 - _), -0.5)
        case WeightMethod.skat =>
          val beta = lbeta(1.0, 25.0)
          mafDV.map(m => 1.0/exp(beta) * pow(1 - m, 24.0))
        case _ => DenseVector.fill[Double](mafDV.length)(1.0)
      }
    }
  }

  sealed trait LearnedWeight[A] extends BRV[A] {
    def y: DenseVector[Double]
    def cov: Option[DenseMatrix[Double]]
    def weight = {
      val geno = getRare(1.1).get.coding.toDense
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

  case class DefaultRaw[A: Genotype](vars: Array[Variant[A]],
                           config: MethodConfig) extends Encode[A] with Raw[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafRaw[A: Genotype](vars: Array[Variant[A]],
                            controls: Array[Boolean],
                            config: MethodConfig)
    extends Encode[A] with Raw[A] with ControlsMaf[A]

  case class DefaultSingle[A: Genotype](vars: Array[Variant[A]],
                           config: MethodConfig)
    extends Encode[A] with Single[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafSingle[A: Genotype](vars: Array[Variant[A]],
                               controls: Array[Boolean],
                               config: MethodConfig)
    extends Encode[A] with Single[A] with ControlsMaf[A]

  case class DefaultCMC[A: Genotype](vars: Array[Variant[A]],
                        config: MethodConfig)
    extends Encode[A] with CMC[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafCMC[A: Genotype](vars: Array[Variant[A]],
                            controls: Array[Boolean],
                            config: MethodConfig)
    extends Encode[A] with CMC[A] with ControlsMaf[A]

  case class SimpleBRV[A: Genotype](vars: Array[Variant[A]],
                       config: MethodConfig)
    extends Encode[A] with BRV[A] with PooledOrAnnotationMaf[A] with SimpleWeight[A]


  case class ControlsMafSimpleBRV[A: Genotype](vars: Array[Variant[A]],
                                  controls: Array[Boolean],
                                  config: MethodConfig)
    extends Encode[A] with BRV[A] with ControlsMaf[A] with SimpleWeight[A]

  case class ErecBRV[A: Genotype](vars: Array[Variant[A]],
                     y: DenseVector[Double],
                     cov: Option[DenseMatrix[Double]],
                     config: MethodConfig)
    extends Encode[A] with BRV[A] with PooledOrAnnotationMaf[A] with LearnedWeight[A]

  case class ControlsMafErecBRV[A: Genotype](vars: Array[Variant[A]],
                                controls: Array[Boolean],
                                y: DenseVector[Double],
                                cov: Option[DenseMatrix[Double]],
                                config: MethodConfig)
    extends Encode[A] with BRV[A] with ControlsMaf[A] with LearnedWeight[A]


}

@SerialVersionUID(7727390001L)
abstract class Encode[A: Genotype] extends Serializable {

  def genotype = implicitly[Genotype[A]]

  def getNew(newY: DenseVector[Double]): Encode[A] = this match {
    case Encode.ErecBRV(vars, _, cov, config) =>
      Encode.ErecBRV(vars, newY, cov, config)
    case Encode.ControlsMafErecBRV(vars, controls, _, cov, config) =>
      Encode.ControlsMafErecBRV(vars, controls, newY, cov, config)
    case x => x
  }
  def vars: Array[Variant[A]]
  def monoallelic: Boolean = {
    vars.map(v => v.toCounter(genotype.toAAF, (0.0, 2.0)).reduce).forall(mc =>
      mc._1 == 0.0 || mc._1 == mc._2
    )
  }
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
    val sortedMaf = maf.filter(m => m < fixedCutoff || m > (1 - fixedCutoff))
      .map(m => if (m < 0.5) m else 1 - m).sorted
    if (sortedMaf.isEmpty) {
      //println(s"sortedMaf length should be 0 == ${sortedMaf.length}")
      None
    } else {
      //println(s"sortedMaf length should be 0 < ${sortedMaf.length}")
      Some(sortedMaf.map(c => Array(c)).reduce{(a, b) =>
        //println(s"a: ${a.mkString(",")} b: ${b.mkString(",")}")
        if (a.last + tol >= b(0))
          a.slice(0, a.length - 1) ++ b
        else
          a ++ b})
    }
  }
  def weight: DenseVector[Double]
  def getFixed(cutoff: Double = fixedCutoff): Option[Encode.Fixed]

  def getVT: Option[Encode.VT] = {
    val tol = 4.0/(9 * sampleSize)
    thresholds.map{th =>
      val cm = SparseVector.horzcat(th.map(c => this.getFixed(c + tol).get.coding): _*)
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
    if (maf.forall(m => m < cutoff || m > (1 - cutoff))) {
      None
    } else {
      val res = DenseVector.horzcat(vars.zip(maf).filter(v =>
        v._2 >= cutoff && v._2 <= (1 - cutoff)).map {
        case (v, m) => DenseVector(v.toIndexedSeq.map(genotype.toBRV(_, m)): _*)
      }: _*)
      val info = vars.zip(mafCount)
        .filter(v => v._2.ratio >= cutoff && v._2.ratio <= (1 - cutoff)).map{
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
    if (maf.exists(m => m < cutoff || m > (1 - cutoff))) {
      val builder = new CSCMatrix.Builder[Double](sampleSize, maf.count(m => m < cutoff || m > (1 - cutoff)))
      val cnt = vars.zip(maf).filter(v => v._2 < cutoff || v._2 > (1 - cutoff)).map{ v =>
        v._1.toCounter(genotype.toBRV(_, v._2), 0.0).toMap}
      cnt.zipWithIndex.foreach{ case (elems, col) =>
          elems.foreach{case (row, v) => builder.add(row, col, v)}}
      val info = vars.zip(mafCount).filter(v => v._2.ratio < cutoff || v._2.ratio > (1 -cutoff)).map{
        case (v, m) =>
          v.toVariation().addInfo(Constant.Variant.InfoKey.maf, s"${m._1},${m._2}")
      }
      Some(Rare(builder.result, info))
    } else {
      None
    }
  }
}


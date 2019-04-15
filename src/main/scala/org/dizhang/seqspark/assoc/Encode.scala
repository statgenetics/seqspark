/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.assoc


import breeze.linalg.{CSCMatrix, DenseMatrix, DenseVector, SparseVector, sum}
import breeze.numerics.{exp, lbeta, pow}
import com.typesafe.config.ConfigFactory
import org.dizhang.seqspark.assoc.Encode._
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.stat.{LinearRegression, LogisticRegression}
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.Constant.Variant.InfoKey
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.util.ConfigValue._

import scala.collection.mutable


/**
  * How to code variants, either in one group (gene) or in a region
  * This class only applies to burden test
  * For meta and Skat, use the raw encoding
  *
  */

object Encode {

  type Imputed = (Double, Double, Double)

  val DummySV = SparseVector.fill[Double](1)(0.0)
  val DummySM = CSCMatrix.fill(1,1)(0.0)
  val DummyDV = DenseVector.fill(1)(0.0)
  val DummyVars = Array.empty[Variation]
  val DummyFixed = Fixed(DummyDV, DummyVars)
  val DummyVT = VT(Array(DummyDV), DummyVars)

  implicit class AF(val f: Double) extends AnyVal {
    def isRare(cutoff: Double): Boolean = f < cutoff || f > (1 - cutoff)
    def isCommon(cutoff: Double): Boolean = ! isRare(cutoff)
  }

  object CmcAddNaAdjust extends SemiGroup[Double] {
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


  val BrvAddNaAdjust = SemiGroup.AtomDouble

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
                         sm: String): Encode[A] = {
    val config = MethodConfig(ConfigFactory.parseString(sm))
    val codingScheme = config.`type`
    val mafSource = config.maf.getString("source")
    val weightMethod = config.weight
    (codingScheme, mafSource, weightMethod) match {
      case (MethodType.brv, "controls", WeightMethod.erec) =>
        apply(vars, controls.get, y.get, cov, sm)
      case (MethodType.brv, _, WeightMethod.erec) =>
        apply(vars, y.get, cov, sm)
      case (_, "controls", _) =>
        apply(vars, controls.get, sm)
      case (_, _, _) =>
        apply(vars, sm)
    }
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
                         sm: String): Encode[A]= {
    val vars = varsIter.toArray
    val config = MethodConfig(ConfigFactory.parseString(sm))
    config.`type` match {
      case MethodType.snv => DefaultSingle(vars, sm)
      case MethodType.cmc => DefaultCMC(vars, sm)
      case MethodType.brv => SimpleBRV(vars, sm)
      case MethodType.skat => SimpleBRV(vars, sm)
      case MethodType.skato => SimpleBRV(vars, sm)
      case MethodType.meta => DefaultRaw(vars, sm)
      case _ => DefaultCMC(vars, sm)
    }
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
                         controls: Array[Boolean],
                         sm: String): Encode[A]= {
    val vars = varsIter.toArray
    val config = MethodConfig(ConfigFactory.parseString(sm))
    config.`type` match {
      case MethodType.snv => ControlsMafSingle(vars, controls, sm)
      case MethodType.cmc => ControlsMafCMC(vars, controls, sm)
      case MethodType.brv => ControlsMafSimpleBRV(vars, controls, sm)
      case MethodType.skat => ControlsMafSimpleBRV(vars, controls, sm)
      case MethodType.skato => ControlsMafSimpleBRV(vars, controls, sm)
      case MethodType.meta => ControlsMafRaw(vars, controls, sm)
      case _ => ControlsMafCMC(vars, controls, sm)
    }
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
                         y: DenseVector[Double],
                         cov: Option[DenseMatrix[Double]],
                         sm: String): Encode[A] = {
    ErecBRV(varsIter.toArray, y, cov, sm)
  }

  def apply[A: Genotype](varsIter: Iterable[Variant[A]],
                         controls: Array[Boolean],
                         y: DenseVector[Double],
                         cov: Option[DenseMatrix[Double]],
                         sm: String): Encode[A] = {
    val vars = varsIter.toArray
    ControlsMafErecBRV(vars, controls, y, cov, sm)
  }

  sealed trait Coding {
    def isDefined: Boolean
    def informative: Boolean
    def copy: Coding
    def size: Int
    def numVars: Int
  }
  sealed trait Meta extends Coding

  case object Empty extends Coding {
    def isDefined = false
    def informative = false
    def copy = Empty
    def size = 0
    def numVars = 0
  }
  case class Rare(coding: CSCMatrix[Double], vars: Array[Variation]) extends Coding with Meta {
    def isDefined = vars.length > 0
    def informative = true
    def copy = {
      Rare(coding.copy, vars.map(v => v.copy()))
    }
    def size = vars.length
    def numVars = vars.length

    override def toString: String = {
      size.toString + "," + coding.activeIterator.map{
        case ((i,j), v) => s"$i,$j,$v"
      }.mkString(",")
    }
  }
  case class Common(coding: DenseMatrix[Double], vars: Array[Variation]) extends Coding with Meta {
    def isDefined = vars.length > 0
    def informative = true
    def copy = Common(coding.copy, vars.map(v => v.copy()))
    def size = vars.length
    def numVars = vars.length
  }
  case class Mixed(rare: Rare, common: Common) extends Coding with Meta {
    def isDefined = rare.isDefined && common.isDefined
    def informative = true
    def copy = Mixed(rare.copy, common.copy)
    def size = rare.size + common.size
    def numVars = rare.numVars + common.numVars
  }
  case class Fixed(coding: DenseVector[Double], vars: Array[Variation]) extends Coding {
    def isDefined = vars.length > 0
    def informative = sum(coding) >= 3.0
    def copy = Fixed(coding.copy, vars.map(v => v.copy()))
    def size = 1
    def numVars = vars.length

    override def toString: String = {
      coding.toArray.mkString(",")
    }
  }
  case class VT(coding: Array[DenseVector[Double]], vars: Array[Variation]) extends Coding {
    def isDefined = vars.length > 0
    def informative = coding.length > 1
    def copy = VT(coding.map(sv => sv.copy), vars.map(v => v.copy()))
    def size = coding.length
    def numVars = vars.length

    override def toString: String = {
      size.toString + "," + coding.map(dv =>
        dv.toArray.mkString(",")
      ).mkString(",")
    }
  }

  sealed trait Raw[A] extends Encode[A] {
    lazy val isDefined = maf.exists(m => m > 0.0 && m < 1.0)
    override def informative(cutoff: Double) = true
    def getFixedBy(cutoff: Double = fixedCutoff) = Fixed(DummyDV, DummyVars)
    def weight = DummyDV
  }

  sealed trait Single[A] extends Encode[A] {
    override def informative(cutoff: Double) = true
    def weight = DummyDV
    lazy val isDefined = maf.exists(m => m.isCommon(fixedCutoff))
    def getFixedBy(cutoff: Double = fixedCutoff) = Fixed(DummyDV, DummyVars)
  }

  sealed trait CMC[A] extends Encode[A] {
    lazy val isDefined = maf.exists(_.isRare(fixedCutoff))
    def weight = DummyDV
    def getFixedBy(cutoff: Double = fixedCutoff): Fixed = {
      definedIndices(_.isRare(cutoff)).map{idx =>
        val dv = idx.map{i =>
          vars(i).toCounter(genotype.toCMC(_, maf(i)), 0.0)
        }.reduce((a, b) => a.++(b)(CmcAddNaAdjust)).toDenseVector(x => x)
        val variations = idx.map{ i =>
          val mc = mafCount(i)
          val res = vars(i).toVariation()
          res.addInfo(InfoKey.mac, s"${mc._1},${mc._2}")
        }
        Fixed(dv, variations)
      }
    }.getOrElse(DummyFixed)
  }

  sealed trait BRV[A] extends Encode[A] {
    lazy val isDefined = maf.exists(_.isRare(fixedCutoff))

    def getFixedBy(cutoff: Double = fixedCutoff): Fixed = {
      val w = weight
      definedIndices(_.isRare(cutoff)).map{idx =>
        val dv = idx.map{i =>
          //println(s"i:$i vars len: ${vars.length} maf len: ${maf.length} weight len: ${w.length}")
          vars(i).toCounter(genotype.toBRV(_, maf(i)) * w(i), 0.0)
        }.reduce((a, b) => a.++(b)(BrvAddNaAdjust)).toDenseVector(x => x)
        val variations = idx.map{ i =>
          val mc = mafCount(i)
          val res = vars(i).toVariation()
          res.addInfo(InfoKey.mac, s"${mc._1},${mc._2}")
        }
        Fixed(dv, variations)
      }.getOrElse(DummyFixed)
    }
  }

  sealed trait PooledOrAnnotationMaf[A] extends Encode[A] {
    lazy val mafCount = vars.map(v => v.toCounter(genotype.toAAF, (0.0, 2.0)).reduce)

    lazy val maf = {
      config.maf.getString("source") match {
        case "pooled" => mafCount.map(_.ratio)
        case key =>
          vars.map{v =>
            val info = v.parseInfo
            if (! info.contains(key)) {
              logger.warn(s"$key found in the INFO field, fall back to sample maf")
              val mac = v.toCounter(genotype.toAAF, (0.0, 2.0)).reduce
              mac._1/mac._2
            } else {
              info(key).toDouble
            }

          }
      }
    }
  }

  sealed trait ControlsMaf[A] extends Encode[A] {
    def controls: Array[Boolean]
    lazy val mafCount = vars.map(v => v.select(controls).toCounter(genotype.toAAF, (0.0, 2.0)).reduce)
    lazy val maf = mafCount.map(_.ratio)

  }

  // TODO: need to add support for arbitary INFO key
  sealed trait SimpleWeight[A] extends BRV[A] {
    def weight: DenseVector[Double] = {
      val mafDV: DenseVector[Double] = DenseVector(maf)
      config.weight match {
        case WeightMethod.annotation =>
          DenseVector(vars.map(v => v.parseInfo(Constant.Variant.InfoKey.weight).toDouble))
        case WeightMethod.wss =>
          pow(mafDV *:* mafDV.map(1.0 - _), -0.5)
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
                           sm: String)
    extends Encode[A] with Raw[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafRaw[A: Genotype](vars: Array[Variant[A]],
                            controls: Array[Boolean],
                            sm: String)
    extends Encode[A] with Raw[A] with ControlsMaf[A]

  case class DefaultSingle[A: Genotype](vars: Array[Variant[A]],
                           sm: String)
    extends Encode[A] with Single[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafSingle[A: Genotype](vars: Array[Variant[A]],
                               controls: Array[Boolean],
                               sm: String)
    extends Encode[A] with Single[A] with ControlsMaf[A]

  case class DefaultCMC[A: Genotype](vars: Array[Variant[A]],
                        sm: String)
    extends Encode[A] with CMC[A] with PooledOrAnnotationMaf[A]

  case class ControlsMafCMC[A: Genotype](vars: Array[Variant[A]],
                            controls: Array[Boolean],
                            sm: String)
    extends Encode[A] with CMC[A] with ControlsMaf[A]

  case class SimpleBRV[A: Genotype](vars: Array[Variant[A]],
                       sm: String)
    extends Encode[A] with BRV[A] with PooledOrAnnotationMaf[A] with SimpleWeight[A]


  case class ControlsMafSimpleBRV[A: Genotype](vars: Array[Variant[A]],
                                  controls: Array[Boolean],
                                  sm: String)
    extends Encode[A] with BRV[A] with ControlsMaf[A] with SimpleWeight[A]

  case class ErecBRV[A: Genotype](vars: Array[Variant[A]],
                     y: DenseVector[Double],
                     cov: Option[DenseMatrix[Double]],
                     sm: String)
    extends Encode[A] with BRV[A] with PooledOrAnnotationMaf[A] with LearnedWeight[A]

  case class ControlsMafErecBRV[A: Genotype](vars: Array[Variant[A]],
                                controls: Array[Boolean],
                                y: DenseVector[Double],
                                cov: Option[DenseMatrix[Double]],
                                sm: String)
    extends Encode[A] with BRV[A] with ControlsMaf[A] with LearnedWeight[A]

}

@SerialVersionUID(7727390001L)
abstract class Encode[A: Genotype] extends Serializable {

  def genotype = implicitly[Genotype[A]]

  def getNew(newY: DenseVector[Double]): Encode[A] = this match {
    case Encode.ErecBRV(vars, _, cov, sm) =>
      Encode.ErecBRV(vars, newY, cov, sm)
    case Encode.ControlsMafErecBRV(vars, controls, _, cov, sm) =>
      Encode.ControlsMafErecBRV(vars, controls, newY, cov, sm)
    case x => x
  }
  def vars: Array[Variant[A]]
  def informative(cutoff: Double = 3.0): Boolean = {
    val mut = sum(getFixed.coding)
    mut >= cutoff
  }
  def mafCount: Array[(Double, Double)]
  def maf: Array[Double]
  def sampleSize: Int = if (vars.isEmpty) 0 else vars.head.length
  def sm: String
  def config: MethodConfig = MethodConfig(ConfigFactory.parseString(sm))
  lazy val fixedCutoff: Double = config.maf.getDouble("cutoff")
  def thresholds: Array[Double] = {
    val n = sampleSize
    /** this is to make sure 90% call rate sites is considered the same with the 100% cr ones
      * (with 1 - 4 minor alleles. anyway 5 is indistinguishable.
      * Because 5.0/0.9n - 5.0/n == 5.0/n - 4.0/0.9n */
    val tol = 4.0/(9 * n)
    val sortedMaf = maf.filter(m => m < fixedCutoff || m > (1 - fixedCutoff))
      .map(m => if (m < 0.5) m else 1 - m).sorted
    if (sortedMaf.isEmpty) {
      //println(s"sortedMaf length should be 0 == ${sortedMaf.length}")
      Array.empty[Double]
    } else {
      //println(s"sortedMaf length should be 0 < ${sortedMaf.length}")
      sortedMaf.map(c => Array(c)).reduce{(a, b) =>
        //println(s"a: ${a.mkString(",")} b: ${b.mkString(",")}")
        if (a.last + tol >= b(0))
          a.slice(0, a.length - 1) ++ b
        else
          a ++ b}
    }
  }

  def weight: DenseVector[Double]
  def getFixedBy(cutoff: Double = fixedCutoff): Encode.Fixed

  lazy val getFixed: Encode.Fixed = getFixedBy()

  lazy val getVT: Encode.VT = {
    val tol = 4.0/(9 * sampleSize)
    getData(_.isRare(fixedCutoff), useWeight = true) match {
      case None => Encode.VT(Array(DummyDV), DummyVars)
      case Some((cnt, vs)) =>
        val mf = vs.map{v =>
          val mc = v.parseInfo(Constant.Variant.InfoKey.mac).split(",")
          val af = mc(0).toDouble/mc(1).toDouble
          if (af < 0.5) af else 1.0 - af
        }
        val sorted = cnt.zip(mf).sortBy(_._2)
        val res = new mutable.ArrayBuffer[Counter[Double]]
        for (i <- sorted.indices) {
          if (i == 0) {
            res += sorted(i)._1
          } else if (sorted(i - 1)._2 + tol >= sorted(i)._2) {
            res(res.length - 1) = res.last ++ sorted(i)._1
          } else {
            res += res.last ++ sorted(i)._1
          }
        }
        val sva = res.map(c => c.toDenseVector(identity)).toArray
        Encode.VT(sva, vs)
    }
  }

  def getCoding: Coding = {
    val res =
      (config.`type`, config.maf.getBoolean("fixed")) match {
        case (MethodType.snv, _) =>
          getCommon() match {case Some(c) => c; case None => Empty}
        case (MethodType.meta, _) =>
          (getCommon(), getRare(useWeight = false)) match {
            case (Some(c), Some(r)) => Mixed(r, c)
            case (Some(c), None) => c
            case (None, Some(r)) => r
            case _ => Empty
          }
        case (MethodType.cmc|MethodType.brv, true) => getFixed
        case (MethodType.cmc|MethodType.brv, false) => getVT
        case (MethodType.skat|MethodType.skato, _) =>
          getRare() match {
            case Some(r) => if (r.numVars > 1) r else Empty
            case None => Empty
          }
        case _ => Empty
      }
    if (res.numVars < config.misc.varLimit._2 && res.numVars >= config.misc.varLimit._1)
      res
    else
      Empty
  }

  def isDefined: Boolean

  def definedIndices(p: Double => Boolean): Option[Array[Int]] = {
    val definedMaf = maf.zipWithIndex.filter(x => p(x._1))
    if (definedMaf.isEmpty) {
      None
    } else {
      Some(definedMaf.map(_._2))
    }
  }

  private def getData(p: Double => Boolean,
                      useWeight: Boolean): Option[(Array[Counter[Double]], Array[Variation])] = {
    val scale = if (useWeight) {
      weight
    } else {
      DenseVector.ones[Double](vars.length)
    }
    definedIndices(p).map{indices =>
      val cnt = for (i <- indices) yield {
        vars(i).toCounter(genotype.toBRV(_, maf(i)), 0.0).map(x => x * scale(i))
      }
      val vs = for (i <- indices) yield {
        val mc = mafCount(i)
        vars(i).toVariation().addInfo(Constant.Variant.InfoKey.mac, s"${mc._1.toInt},${mc._2.toInt}")
      }
      (cnt, vs)
    }
  }

  def getCommon(cutoff: Double = fixedCutoff): Option[Common] = {
    getData(_.isCommon(cutoff), useWeight = false).map{
      case (cnt, vs) =>
        val dvs = cnt.map(_.toDenseVector(x => x))
        Common(DenseVector.horzcat(dvs: _*), vs)
    }
  }

  def getRare(cutoff: Double = fixedCutoff, useWeight: Boolean = true): Option[Encode.Rare] = {
    getData(_.isRare(cutoff), useWeight).map{
      case (cnt, vs) =>
        val svs = cnt.map(c => c.toSparseVector(x => x))
        Rare(SparseVector.horzcat(svs: _*), vs)
    }
  }
}


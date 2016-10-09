package org.dizhang.seqspark.ds

import org.dizhang.seqspark.util.Constant.Genotype.rawToSimple
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.LogicalExpression
/**
  * Created by zhangdi on 9/26/16.
  */
/** here we go */
@SerialVersionUID(101L)
sealed trait Genotype[A] extends Serializable {
  def toAAF(g: A): (Double, Double) //This is the alt allele frequency
  def toCMC(g: A, af: Double): Double
  def toBRV(g: A, af: Double): Double
  def callRate(g: A): (Double, Double)
  def isDiploid(g: A): Boolean
  def isPhased(g: A): Boolean
  def isMis(g: A): Boolean
  def isRef(g: A): Boolean
  def isMut(g: A): Boolean
  def isHet(g: A): Boolean
  def toHWE(g: A): Genotype.Imp
}

object Genotype {

  /** We implement three Genotypes here,
    *   Raw for the raw VCF format,
    *   Simple for the cleaned compressed version,
    *   Imp for imputed data, Impute2 output format
    *   */
  type Imp = (Double, Double, Double)
  implicit object Simple extends Genotype[Byte] {
    def gt(g: Byte): Int = (g << 30) >>> 30
    def toRaw(g: Byte): String = {
      val a1 = (g << 30) >>> 31
      val a2 = (g << 31) >>> 31
      val ctrl = g & 28 //g & b00011100, extract the three control bits
      ctrl match {
        case 28 => ".|."
        case 24 => s"$a1|$a2"
        case 20 => "./."
        case 16 => s"$a1/$a2"
        case 4 => "."
        case _ => a2.toString
      }
    }
    def a1(g: Byte) = (g << 30) >>> 31
    def a2(g: Byte) = (g << 31) >>> 31
    def isMis(g: Byte): Boolean = (g & 4) == 4
    def isPhased(g: Byte): Boolean = (g & 8) == 8
    def isDiploid(g: Byte): Boolean = (g & 16) == 16
    def isHet(g: Byte): Boolean = (! isMis(g)) && isDiploid(g) && (a1(g) + a2(g) == 1)
    def isRef(g: Byte): Boolean = (! isMis(g)) && (a1(g) + a2(g) == 0)
    def isMut(g: Byte): Boolean = (! isMis(g)) && (a1(g) + a2(g) >= 2)
    def callRate(g: Byte): (Double, Double) = {
      val ctrl = g & 20 //g & b00010100, extract diploid and missing bits
      ctrl match {
        case 20 => (0, 2) //b00010100, diploid and missing
        case 16 => (2, 2) //b00010000, diploid, not missing
        case 4 => (0, 1) //b00000100, monoploid and missing
        case _ => (1, 1) //b00000000, monoploid, not missing
      }
    }
    def toAAF(g: Byte): (Double, Double) = {
      val a1 = (g << 30) >>> 31
      val a2 = (g << 31) >>> 31
      val cnt = a1 + a2
      val ctrl = g & 20 //similar with callRate
      ctrl match {
        case 16 => (cnt, 2) //diploid, not missing
        case 0 => (a2, 1) //monoploid, not missing
        case _ => (0, 0) //missing
      }
    }
    def toHWE(g: Byte): (Double, Double, Double) = {
      if (isMis(g)) {
        (0, 0, 0)
      } else if (a1(g) != a2(g)) {
        (0, 1, 0)
      } else if (a1(g) == 0) {
        (1, 0, 0)
      } else {
        (0, 0, 1)
      }
    }
    def toCMC(g: Byte, af: Double) = {
      if (af < 0.5) {
        if (this.isMis(g)) {
          1.0 - (1.0 - af).square
        } else if (this.gt(g) == 0) {
          0.0
        } else {
          1.0
        }
      } else {
        if (this.isMis(g)) {
          1.0 - af.square
        } else if (this.gt(g) == 3) {
          0.0
        } else {
          1.0
        }
      }
    }
    def toBRV(g: Byte, af: Double) = {
      if (af < 0.5) {
        if (this.isMis(g)) {
          2 * af
        } else if (this.gt(g) == 0) {
          0.0
        } else if (this.isHet(g)) {
          1.0
        } else {
          2.0
        }
      } else {
        if (this.isMis(g)) {
          2 * (1.0 -af)
        } else if (this.gt(g) == 3) {
          0.0
        } else if (this.isHet(g)) {
          1.0
        } else {
          2.0
        }
      }
    }
  }
  implicit object Raw extends Genotype[String] {
    def gt(g: String): String = g.split(":")(0)
    def toSimpleGenotype(g: String): Byte = {
      rawToSimple(gt(g))
    }
    def isMis(g: String): Boolean = g.startsWith(".")
    def isPhased(g: String): Boolean = gt(g).contains("|")
    def isDiploid(g: String): Boolean = gt(g).split("[/|]").length == 2
    def isHet(g: String): Boolean = Simple.isHet(toSimpleGenotype(g))
    def isRef(g: String): Boolean = Simple.isRef(toSimpleGenotype(g))
    def isMut(g: String): Boolean = Simple.isMut(toSimpleGenotype(g))
    def toCMC(g: String, af: Double) = Simple.toCMC(toSimpleGenotype(g), af)
    def toBRV(g: String, af: Double) = Simple.toBRV(toSimpleGenotype(g), af)

    def fields(g: String, format: String): Map[String, String] = {
      val s = g.split(":")
      val f = format.split(":")
      if (s.length == f.length) {
        f.zip(s).flatMap{
          case (k, v) =>
            val vs = v.split(",")
            if (vs == 2) {
              Array((k + "_1", vs(0)), (k + "_2", vs(1)), (k + "_ratio", (vs(0).toDouble/vs(1).toDouble).toString))
            } else {
              Array((k, v))
            }
        }.toMap
      } else {
        Map(f(0) -> s(0))
      }
    }

    def qc(g: String, format: String, cond: String, mis: String): String = {
      val varMap = fields(g, format).withDefaultValue("0")
      if (LogicalExpression.judge(varMap)(cond)) {
        g
      } else {
        mis
      }
    }

    def callRate(g: String): (Double, Double) = {
      if (g.startsWith("."))
        (0, 1)
      else
        (1, 1)
    }
    def toAAF(g: String): (Double, Double) = {
      val gt = g.split(":")(0).split("[/|]").map(_.toInt)
      if (gt.length == 1) {
        (gt(0), 1)
      } else {
        (gt.sum, 2)
      }
    }
    def toHWE(g: String): (Double, Double, Double) = {
      if (isMis(g)) {
        (0, 0, 0)
      } else if (isHet(g)) {
        (0, 1, 0)
      } else if (g.startsWith("0")) {
        (1, 0, 0)
      } else {
        (0, 0, 1)
      }
    }

  }
  implicit object Imputed extends Genotype[Imp] {
    /**
      * the support for imputed genotype is limited
      * on chromosome X, the callrate and aaf may be incorrect for males
      * */
    def isMis(g: Imp): Boolean = g._1 + g._2 + g._3 == 0.0
    def isPhased(g: Imp): Boolean = false
    def isDiploid(g: Imp): Boolean = true
    def isRef(g: Imp): Boolean = g._1 >= (1.0 - 1e-2)
    def isHet(g: Imp): Boolean = g._2 >= (1.0 - 1e-2)
    def isMut(g: Imp): Boolean = g._3 >= (1.0 - 1e-2)
    def toHWE(g: Imp): Imp = g
    def callRate(g: Imp): (Double, Double) = {
      if (g._1 + g._2 + g._3 == 1.0)
        (2, 2)
      else
        (0, 2)
    }
    def toAAF(g: Imp): (Double, Double) = {
      (g._2 + 2 * g._3, 2)
    }
    def toCMC(g: Imp, af: Double): Double = {
      if (af < 0.5) {
        if (this.isMis(g)) {
          1.0 - (1.0 - af).square
        } else {
          g._2 + g._3
        }
      } else {
        if (this.isMis(g)) {
          1.0 - af.square
        } else {
          g._1 + g._2
        }
      }
    }
    def toBRV(g: Imp, af: Double): Double = {
      if (af < 0.5) {
        if (this.isMis(g)) {
          2 * af
        } else {
          g._2 + 2 * g._3
        }
      } else {
        if (this.isMis(g)) {
          2 * (1.0 - af)
        } else {
          g._2 + 2 * g._1
        }
      }
    }

  }
}

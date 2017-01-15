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

package org.dizhang.seqspark.ds

import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.LogicalParser
/**
  * Created by zhangdi on 9/26/16.
  */
/** here we go */
@SerialVersionUID(101L)
sealed trait Genotype[A] extends Serializable {
  def toAAF(g: A): (Double, Double) //This is the alt allele frequency
  def toCMC(g: A, af: Double): Double
  def toBRV(g: A, af: Double): Double
  def toPCA(g: A, af: Double): Double = {
    val maf = if (af <= 0.5) af else 1.0 - af
    (toBRV(g, af) - 2 * maf)/(af * (1.0 - af))
  }
  def callRate(g: A): (Double, Double)
  def isDiploid(g: A): Boolean
  def isPhased(g: A): Boolean
  def isMis(g: A): Boolean
  def isRef(g: A): Boolean
  def isMut(g: A): Boolean
  def isHet(g: A): Boolean
  def toHWE(g: A): Genotype.Imp
  def toVCF(g: A): String
}

object Genotype {

  /** We implement three Genotypes here,
    *   Raw for the raw VCF format,
    *   Simple for the cleaned compressed version,
    *   Imp for imputed data, Impute2 output format
    *   */
  type Imp = (Double, Double, Double)
  private val diploidGT = """([0-9.])([/|])([0-9.])(?::.+)?""".r
  private val monoGT = """([0-9.])(?::.+)?""".r
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
    def toVCF(g: Byte) = toRaw(g)
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
        case 20 => (0, 1) //b00010100, diploid and missing
        case 16 => (1, 1) //b00010000, diploid, not missing
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

    def toVCF(g: String) = gt(g)

    def gt(g: String): String = g.split(":")(0)

    def toSimpleGenotype(g: String): Byte = {

      g match {
        case diploidGT(a1, sep, a2) =>
          val phased = if (sep == "/") 16 else 24
          val gt = if (a1 == ".") 4 else a1.toInt * 2 + a2.toInt
          (gt | phased).toByte
        case monoGT(a) =>
          if (a == ".") 4.toByte else a.toByte
        case _ =>
          20.toByte
      }

      //rawToSimple(gt(g))
    }
    /**
    def rawToSimple(g: String): Byte = {
      /**
      """
        |the simple genotype system uses 5 bits to represent a genotype
        |b00010000: is diploid
        |b00001000: is phased, notice that there is no 8, but 24
        |b00000100: is missing
        |00-11 represent the four possible genotypes
      """.stripMargin
        */


      val diploidPhased = if (g.contains('|')) {
        24 //b00011000
      } else if (g.contains('/')) {
        16 //b00010000
      } else {
        0
      }
      val gt = try {

        g.split("[/|]").map(_.toInt).sum //0-3 for normal genotype
      } catch {
        case e: Exception => 4 //b00000100 for missing
      }

      (diploidPhased | gt).toByte

    }
    */

    def isMis(g: String): Boolean = g.startsWith(".")

    def isPhased(g: String): Boolean = gt(g).contains("|")

    def isDiploid(g: String): Boolean = gt(g).split("[/|]").length == 2

    def isHet(g: String): Boolean = Simple.isHet(toSimpleGenotype(g))

    def isRef(g: String): Boolean = Simple.isRef(toSimpleGenotype(g))

    def isMut(g: String): Boolean = Simple.isMut(toSimpleGenotype(g))

    def toCMC(g: String, af: Double) = Simple.toCMC(toSimpleGenotype(g), af)

    def toBRV(g: String, af: Double) = Simple.toBRV(toSimpleGenotype(g), af)

    def fields(g: String)(is: Int*): IndexedSeq[String] = {
      val s = g.split(":")
      for {i <- 0 until s.length; if is.contains(i)} yield s(i)
    }

    def fields(g: String, format: List[String]): Map[String, String] = {
      val s = g.split(":")
      if (s.length == 1)
        Map(format.head -> s(0))
      else {
        /** remove the missing values */
        format.zip(s).filter(_._2 != ".").toMap
      }

    }

    def qc(g: String, cond: LogicalParser.LogExpr, format: List[String], mis: String): String = {
      val varMap = fields(g, format).withDefaultValue("0")
      if (LogicalParser.eval(cond)(varMap))
        varMap("GT")
      else
        mis
    }

    def callRate(g: String): (Double, Double) = {
      if (g.startsWith("."))
        (0, 1)
      else
        (1, 1)
    }

    def toAAF(g: String): (Double, Double) = {
      val gt = g.split(":")(0).split("[/|]")
      if (gt.length == 1) {
        if (gt(0) == ".") {
          (0, 0)
        } else {
          (gt(0).toInt, 1)
        }
      } else {
        if (gt(0) == ".") {
          (0, 0)
        } else {
          (gt.map(_.toInt).sum, 2)
        }
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
    def toVCF(g: Imp): String = {
      if (g._1 > g._2 && g._1 > g._3) {
        "0/0"
      } else if (g._2 > g._1 && g._2 > g._3) {
        "0/1"
      } else if (g._3 > g._1 && g._3 > g._2) {
        "1/1"
      } else {
        "./."
      }
    }
    def callRate(g: Imp): (Double, Double) = {
      if (g._1 + g._2 + g._3 == 1.0)
        (1, 1)
      else
        (0, 1)
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

package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import SimpleVCF._
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.worker.{Samples, Variants, Data}
import org.dizhang.seqspark.util.General._

/**
  * Created by zhangdi on 9/16/16.
  */
case class SimpleVCF(self: RDD[Variant[Byte]]) extends GeneralizedVCF[Byte] {
  def toRawVCF: Data[String] = {
    self.map(v => v.map(g => g.toRaw))
  }

  def variantsFilter(cond: List[String])(ssc: SingleStudyContext): Data[Byte] = {
    val conf = ssc.userConfig
    val pheno = ssc.phenotype
    val batch = pheno.batch(conf.input.phenotype.batch)
    val controls = pheno.select("control").map{
      case Some("1") => true
      case _ => false
    }
    val myCond = cond.map(c => s"( $c )").reduce((a, b) => s"$a and $b")
    Variants.filter[Byte](self, myCond, batch, controls, _.maf, _.callRate, _.hwe)
  }

  def checkSex(): Unit = {
    def isHet(g: Byte): (Double, Double) = {
      if (g.isMis) {
        (0, 0)
      } else if (g.isHet) {
        (1, 1)
      } else {
        (0, 1)
      }
    }
    Samples.checkSex[Byte](self, isHet, _.callRate)
  }
}

object SimpleVCF {
  implicit class SimpleGenotype(val g: Byte) extends AnyVal {
    def gt: Int = (g << 30) >>> 30
    def toRaw: String = {
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
    def a1 = (g << 30) >>> 31
    def a2 = (g << 31) >>> 31
    def isMis: Boolean = (g & 4) == 4
    def isDiploid: Boolean = (g & 16) == 16
    def isHet: Boolean = (! isMis) && isDiploid && a1 != a2
    def callRate: (Double, Double) = {
      val ctrl = g & 20 //g & b00010100, extract diploid and missing bits
      ctrl match {
        case 20 => (0, 2) //b00010100, diploid and missing
        case 16 => (2, 2) //b00010000, diploid, not missing
        case 4 => (0, 1) //b00000100, monoploid and missing
        case _ => (1, 1) //b00000000, monoploid, not missing
      }
    }
    def maf: (Double, Double) = {
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
    def hwe: (Double, Double, Double) = {
      if (isMis) {
        (0, 0, 0)
      } else if (a1 != a2) {
        (0, 1, 0)
      } else if (a1 == 0) {
        (1, 0, 0)
      } else {
        (0, 0, 1)
      }
    }
    def toCMC(maf: Double) = {
      if (maf < 0.5) {
        if (this.isMis) {
          1.0 - (1.0 - maf).square
        } else if (this.gt == 0) {
          0.0
        } else {
          1.0
        }
      } else {
        if (this.isMis) {
          1.0 - maf.square
        } else if (this.gt == 3) {
          0.0
        } else {
          1.0
        }
      }
    }
    def toBRV(maf: Double) = {
      if (maf < 0.5) {
        if (this.isMis) {
          2 * maf
        } else if (this.gt == 0) {
          0.0
        } else if (this.isHet) {
          1.0
        } else {
          2.0
        }
      } else {
        if (this.isMis) {
          2 * (1.0 -maf)
        } else if (this.gt == 3) {
          0.0
        } else if (this.isHet) {
          1.0
        } else {
          2.0
        }
      }
    }
  }
}

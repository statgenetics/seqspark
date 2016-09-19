package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import SimpleVCF._

/**
  * Created by zhangdi on 9/16/16.
  */
case class SimpleVCF(self: RDD[Variant[Byte]]) extends GeneralizedVCF[Byte] {
  def toRawVCF: Data[String] = {
    self.map(v => v.map(g => g.toRaw))
  }
}

object SimpleVCF {
  implicit class SimpleGenotype(val g: Byte) extends AnyVal {
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
  }
}

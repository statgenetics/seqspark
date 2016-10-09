package org.dizhang.seqspark.worker

import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.annot.VariantAnnotOp._
import breeze.stats.distributions.ChiSquared

import scala.language.implicitConversions
/**
  * Created by zhangdi on 9/20/16.
  */
object Variants {

  implicit def convertToVQC[A: Genotype](v: Variant[A]): VariantQC[A] = new VariantQC(v)

  def decompose(self: Data[String]): Data[String] = {
    self.flatMap(v => decomposeVariant(v))
  }

  def decomposeVariant(v: Variant[String]): Array[Variant[String]] = {
    /** decompose multi-allelic variants to bi-allelic variants */
    if (v.alleleNum == 2) {
      Array(v)
    } else {
      val alleles = v.alleles
      (1 until v.alleleNum).toArray.map{i =>
        val newV = v.map{g =>
          val s = g.split(":")
          val gt = s(0).split("[|/]")
          if (gt.length == 1) {
            if (gt(0) == "0") "0" else "1"
          } else {
            gt.map(j => if (j.toInt == i) "1" else "0").mkString(s(0).substring(1,2))
          }
        }
        newV.meta(4) = alleles(i)
        newV
      }
    }
  }

  @SerialVersionUID(100L)
  class VariantQC[A: Genotype](v: Variant[A]) extends Serializable {
    def geno = implicitly[Genotype[A]]
    def maf(controls: Array[Boolean]): Double = {
      val res = v.select(controls).toCounter(geno.toAAF, (0.0, 2.0)).reduce
      res.ratio
    }
    def batchMaf(controls: Array[Boolean],
                 batch: Array[String]): Map[String, Double] = {
      val keyFunc = (i: Int) => batch(i)
      val res = v.select(controls).toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
      res.map{case (k, v) => k -> v.ratio}
    }
    def callRate: Double = {
      val res = v.toCounter(geno.callRate, (1.0, 1.0)).reduce
      res.ratio
    }
    def batchCallRate(batch: Array[String]): Map[String, Double] = {
      val keyFunc = (i: Int) => batch(i)
      val res = v.toCounter(geno.callRate, (1.0, 1.0)).reduceByKey(keyFunc)
      res.map{case (k, v) => k -> v.ratio}
    }
    def hwePvalue(controls: Array[Boolean]): Double = {
      val cnt = v.select(controls).toCounter(geno.toHWE, (1.0, 0.0, 0.0)).reduce
      val n = cnt._1 + cnt._2 + cnt._3
      val p = (cnt._1 + cnt._2/2)/n
      val q = 1 - p
      val eAA = p.square * n
      val eAa = 2 * p * q * n
      val eaa = q.square * n
      val chisq = (cnt._1 - eAA).square/eAA + (cnt._2 - eAa).square/eAa + (cnt._3 - eaa).square/eaa
      val dis = ChiSquared(1)
      1.0 - dis.cdf(chisq)
    }

    def batchSpecific(batch: Array[String]): Map[String, Double] = {
      val keyFunc = (i: Int) => batch(i)
      val res = v.toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
      res.map{case (k, v) => k -> math.min(v._1, v._2 - v._1)}
    }

    def isFunctional: Int = {
      val genes = parseAnnotation(v.parseInfo(IK.anno))
      if (genes.exists(p => FM(p._2) <= 4)) 1 else 0
    }
  }

}

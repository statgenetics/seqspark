package org.dizhang.seqspark.worker

import java.io.{File, PrintWriter}

import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.ds.{Genotype, SparseVariant, Variant}
import org.dizhang.seqspark.util.Constant.Variant._
import org.dizhang.seqspark.util.General._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions
/**
  * Created by zhangdi on 9/20/16.
  */
object Variants {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit def convertToVQC[A: Genotype](v: Variant[A]): VariantQC[A] = new VariantQC(v)

  def decompose(self: Data[String]): Data[String] = {
    logger.info("decompose multi-allelic variants")
    self.flatMap(v => decomposeVariant(v))
  }

  def countByFunction[A](self: Data[A]): Unit = {
    val cnt = self.map(v =>
      if (v.parseInfo.contains(IK.anno)) {
        worstAnnotation(v.parseInfo(IK.anno)) -> 1
      } else {
        F.Unknown -> 1
      }).countByKey()
      .toArray.sortBy(p => FM(p._1))

    val pw = new PrintWriter(new File("output/annotation_summary.txt"))
    for ((k, v) <- cnt) {
      pw.write(s"${k.toString}: $v\n")
    }
    pw.close()
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
          if (s(0).contains(".")) {
            s(0)
          } else {
            val gt = s(0).split("[|/]")
            if (gt.length == 1) {
              if (gt(0) == "0") "0" else "1"
            } else {
              gt.map(j => if (j.toInt == i) "1" else "0").mkString(s(0).substring(1,2))
            }
          }
        }
        newV.meta(4) = alleles(i)
        newV
      }.filter{
        case SparseVariant(_,e,_,_) => e.nonEmpty
        case _ => true
      }
    }
  }

  @SerialVersionUID(100L)
  class VariantQC[A: Genotype](v: Variant[A]) extends Serializable {
    def geno = implicitly[Genotype[A]]
    def maf(controls: Option[Array[Boolean]]): Double = {
      val cache = v.parseInfo
      if (cache.contains(InfoKey.maf)) {
        val res = cache(InfoKey.maf).split(",").map(_.toDouble)
        res(0)/res(1)
      } else {
        controls match {
          case None =>
            val res = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce
            v.addInfo(InfoKey.maf,s"${res._1},${res._2}")
            res.ratio
          case Some(indi) =>
            val res = v.select(indi).toCounter(geno.toAAF, (0.0, 2.0)).reduce
            v.addInfo(InfoKey.maf, s"${res._1},${res._2}")
            res.ratio
        }
      }
    }
    def informative: Boolean = {
      val af = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce
      af._1 != 0.0 && af._1 != af._2
    }
    def batchMaf(controls: Option[Array[Boolean]],
                 batch: Array[String]): Map[String, Double] = {
      val keyFunc = (i: Int) => batch(i)
      val rest = controls match {case None => v; case Some(c) => v.select(c)}
      val res = rest.toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
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
    def hwePvalue(controls: Option[Array[Boolean]]): Double = {
      val rest = controls match {
        case Some(c) => v.select(c)
        case None => v
      }
      val cnt =  rest.toCounter(geno.toHWE, (1.0, 0.0, 0.0)).reduce
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
      if (v.parseInfo.contains("dbSNP")) {
        batch.map(b => b -> 0.0).toMap
      } else {
        val keyFunc = (i: Int) => batch(i)
        val res = v.toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
        res.map{case (k, v) => k -> math.min(v._1, v._2 - v._1)}
      }
    }

    def isFunctional: Int = {
      val info = v.parseInfo
      if (info.contains(IK.anno) && info(IK.anno) != F.InterGenic.toString) {
        val genes = parseAnnotation(v.parseInfo(IK.anno))
        if (genes.exists(p => FM(p._2) <= 4)) 1 else 0
      } else {
        0
      }
    }
  }

}

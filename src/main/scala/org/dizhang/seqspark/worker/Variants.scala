package org.dizhang.seqspark.worker

import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.util.{LogicalExpression, SingleStudyContext}
import breeze.stats.distributions.ChiSquared
import org.apache.spark.rdd.RDD
/**
  * Created by zhangdi on 9/20/16.
  */
object Variants {

  def maf[A](v: Variant[A], controls: Array[Boolean], makeMaf: A => (Double, Double)): Double = {
    val res = v.select(controls).toCounter(makeMaf, (0.0, 2.0)).reduce
    res.ratio
  }
  def batchMaf[A](v: Variant[A],
                  controls: Array[Boolean],
                  batch: Array[String],
                  makeMaf: A => (Double, Double)): Map[String, Double] = {
    val keyFunc = (i: Int) => batch(i)
    val res = v.select(controls).toCounter(makeMaf, (0.0, 2.0)).reduceByKey(keyFunc)
    res.map{case (k, v) => k -> v.ratio}
  }
  def callRate[A](v: Variant[A], makeCall: A => (Double, Double)): Double = {
    val res = v.toCounter(makeCall, (1.0, 1.0)).reduce
    res.ratio
  }
  def batchCallRate[A](v: Variant[A],
                       batch: Array[String],
                       makeCall: A => (Double, Double)): Map[String, Double] = {
    val keyFunc = (i: Int) => batch(i)
    val res = v.toCounter(makeCall, (1.0, 1.0)).reduceByKey(keyFunc)
    res.map{case (k, v) => k -> v.ratio}
  }
  def hwePvalue[A](v: Variant[A],
                   controls: Array[Boolean],
                   makeHWE: A => (Double, Double, Double)): Double = {
    val cnt = v.select(controls).toCounter(makeHWE, (1.0, 0.0, 0.0)).reduce
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

  def batchSpecific[A](v: Variant[A],
                       batch: Array[String],
                       makeMaf: A => (Double, Double)): Map[String, Double] = {
    val keyFunc = (i: Int) => batch(i)
    val res = v.toCounter(makeMaf, (0.0, 2.0)).reduceByKey(keyFunc)
    res.map{case (k, v) => k -> math.min(v._1, v._2 - v._1)}
  }

  def isFunctional[A](v: Variant[A]): Int = {
    val genes = parseAnnotation(v.parseInfo(IK.anno))
    if (genes.exists(p => FM(p._2) <= 4)) 1 else 0
  }

  def filter[A](self: Data[A],
                variants: String,
                batch: Array[String],
                controls: Array[Boolean],
                makeMaf: A => (Double, Double),
                makeCall: A => (Double, Double),
                makeHWE: A => (Double, Double, Double)): Data[A] = {

    val names: Set[String] = LogicalExpression.analyze(variants)
    self.filter{v =>
      val varMap = names.toArray.map{
        case "chr" => "chr" -> v.chr
        case "maf" => "maf" -> maf(v, controls, makeMaf).toString
        case "missingRate" => "missingRate" -> (1 - callRate(v, makeCall)).toString
        case "batchMissingRate" => "batchMissingRate" -> (1 - batchCallRate(v, batch, makeCall).values.max).toString
        case "alleleNum" => "alleleNum" -> v.alleleNum.toString
        case "batchSpecific" => "batchSpecific" -> batchSpecific(v, batch, makeMaf).values.max.toString
        case "hwePvalue" => "hwePvalue" -> hwePvalue(v, controls, makeHWE).toString
        case "isFunctional" => "isFunctional" -> isFunctional(v).toString
        case x => x -> v.parseInfo.getOrElse(x, "0")
      }.toMap
      LogicalExpression.judge(varMap)(variants)
    }
  }

}

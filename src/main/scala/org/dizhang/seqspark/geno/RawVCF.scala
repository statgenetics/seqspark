package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.Constant.Genotype._
import org.dizhang.seqspark.util.LogicalExpression
import RawVCF._
/**
  * Created by zhangdi on 8/15/16.
  */
case class RawVCF(self: RDD[Variant[String]]) extends GeneralizedVCF[String] {

  def genotypeQC(cond: String): RDD[Variant[String]] = {
    self.map{v =>
      v.map{g =>
        val mis = if (g.gt.contains('|')) {
          Raw.diploidPhasedMis
        } else if (g.gt.contains('/')) {
          Raw.diploidUnPhasedMis
        } else {
          Raw.monoploidMis
        }
        g.qc(v.format, cond, mis)}}
  }

  def toSimpleVCF: Data[Byte] = {
    self.map(v => v.map(g => g.toSimpleGenotype))
  }

}

object RawVCF {

  implicit class RawGenotype(val g: String) extends AnyVal {

    def gt: String = g.split(":")(0)

    def toSimpleGenotype: Byte = {
      rawToSimple(g.gt)
    }

    def fields(format: String): Map[String, String] = {
      val s = g.split(":")
      val f = format.split(":")
      if (s.length == f.length) {
        f.zip(s).flatMap{
          case (k, v) =>
            val vs = v.split(",")
            if (vs == 2) {
              Array((k + "_1", vs(0)), (k + "_2", vs(1)), (k + "_ratio", vs(0).toDouble/vs(1).toDouble))
            } else {
              Array((k, v))
            }
        }.toMap
      } else {
        Map(f(0) -> s(0))
      }
    }

    def qc(format: String, cond: String, mis: String): String = {
      val varMap = fields(format)
      if (LogicalExpression.judge(varMap)(cond)) {
        g
      } else {
        mis
      }
    }

    def callRate: (Double, Double) = {
      if (g.startsWith(Raw.monoploidMis))
        (0, 1)
      else
        (1, 1)
    }
    def maf: (Double, Double) = {
      val gt = g.split(":")(0).split("[/|]").map(_.toInt)
      if (gt.length == 1) {
        (gt(0), 1)
      } else {
        (gt.sum, 2)
      }
    }

  }


}

package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.worker.{Samples, Variants, Data}
import ImputedVCF._
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.util.General._

/**
  * Created by zhangdi on 9/16/16.
  */
case class ImputedVCF(self: RDD[Variant[(Double, Double, Double)]]) {

  def variantsFilter(cond: List[String])(ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val pheno = ssc.phenotype
    val batch = pheno.batch(conf.input.phenotype.batch)
    val controls = pheno.select("control").map{
      case Some("1") => true
      case _ => false
    }
    val myCond = cond.map(c => s"($c)").reduce((a,b) => s"$a and $b")
    Variants.filter[(Double, Double, Double)](self, myCond, batch, controls, _.maf, _.callRate, x => x)
  }

  def checkSex(): Unit = {
    def isHet(g: (Double, Double, Double)): (Double, Double) = {
      if (g == (0, 0, 0)) {
        (0, 0)
      } else {
        (g._2, 1)
      }
    }
    Samples.checkSex[(Double, Double, Double)](self, isHet, _.callRate)
  }

}

object ImputedVCF {
  implicit class ImputedGenotype(val g: (Double, Double, Double)) extends AnyVal {
    /**
      * the support for imputed genotype is limited
      * on chromosome X, the callrate and maf may be incorrect for males
      * */
    def isMis: Boolean = g._1 + g._2 + g._3 == 0.0
    def callRate: (Double, Double) = {
      if (g._1 + g._2 + g._3 == 1.0)
        (2, 2)
      else
        (0, 2)
    }
    def maf: (Double, Double) = {
      (g._2 + 2 * g._3, 2)
    }
    def toCMC(maf: Double): Double = {
      if (maf < 0.5) {
        if (this.isMis) {
          1.0 - (1.0 - maf).square
        } else {
          g._2 + g._3
        }
      } else {
        if (this.isMis) {
          1.0 - maf.square
        } else {
          g._1 + g._2
        }
      }
    }
    def toBRV(maf: Double): Double = {
      if (maf < 0.5) {
        if (this.isMis) {
          2 * maf
        } else {
          g._2 + 2 * g._3
        }
      } else {
        if (this.isMis) {
          2 * (1.0 - maf)
        } else {
          g._2 + 2 * g._1
        }
      }
    }

  }
}
package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant

/**
  * Created by zhangdi on 9/16/16.
  */
case class ImputedVCF(self: RDD[Variant[(Double, Double, Double)]]) {

}

object ImputedVCF {
  implicit class ImputedGenotype(val g: (Double, Double, Double)) extends AnyVal {
    /**
      * the support for imputed genotype is limited
      * on chromosome X, the callrate and maf may be incorrect for males
      * */
    def callRate: (Double, Double) = {
      if (g._1 + g._2 + g._3 == 1.0)
        (2, 2)
      else
        (0, 2)
    }
    def maf: (Double, Double) = {
      (g._2 + 2 * g._3, 2)
    }
  }
}
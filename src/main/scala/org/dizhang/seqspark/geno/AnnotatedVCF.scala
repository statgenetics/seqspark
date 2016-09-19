package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant

/**
  * Created by zhangdi on 8/17/16.
  */
trait AnnotatedVCF[K,A] {
  val self: RDD[(K, Variant[A])]
  def groupByKey(): RDD[(K, Array[Variant[A]])] = {
    self.groupByKey().map{
      case (k, v) => (k, v.toArray)
    }
  }
}

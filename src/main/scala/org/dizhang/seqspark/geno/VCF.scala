package org.dizhang.seqspark.geno

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.Variant

/**
  * VCF DSL
  */
trait VCF[gt, repr[_]] {
  def self: gt => repr[gt]
  def filter: Regions => repr[gt]
  def select: Array[Boolean] => repr[gt]
}

object VCF {
  trait Filterable[T]
}

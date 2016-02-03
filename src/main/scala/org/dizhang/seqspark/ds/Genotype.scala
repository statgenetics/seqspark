package org.dizhang.seqspark.ds

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.util.Constant.{RawGenotype => CRG, InnerGenotype => CIG}

/**
  * hold the distributed genetype data, RDD[Variant]
  */
sealed trait Genotype {
  def build: String
  def rawData: RDD[Variant[String]]
  def data: RDD[Variant[Byte]]
}

case class StringGenotype(rawData: RDD[Variant[String]], build: String) extends Genotype {
  def data = rawData.map(v => v.map(g => g.bt))
}

case class ByteGenotype(data: RDD[Variant[Byte]], build: String) extends Genotype {
  def rawData = data.map(v => v.map(g => g.toUnPhased))
}
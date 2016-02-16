package org.dizhang.seqspark.ds

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.util.Constant.{RawGenotype => CRG, InnerGenotype => CIG}
import org.dizhang.seqspark.util.ImExConfig

/**
  * hold the distributed genetype data, RDD[Variant]
  */

sealed trait Genotype {
  def config: ImExConfig
  def rawData: RDD[Variant[String]]
  def data: RDD[Variant[Byte]]
}

case class StringGenotype(rawData: RDD[Variant[String]], config: ImExConfig) extends Genotype {
  def data = rawData.map(v => v.map(g => g.bt))
}

case class ByteGenotype(data: RDD[Variant[Byte]], config: ImExConfig) extends Genotype {
  def rawData =
    if (config.phased) {
      data.map(v => v.map(g => g.toUnPhased))
    } else {
      data.map(v => v.map(g => g.toPhased))
    }
}
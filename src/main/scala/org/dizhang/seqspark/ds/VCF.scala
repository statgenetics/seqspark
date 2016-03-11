package org.dizhang.seqspark.ds

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.IntervalTree
import org.dizhang.seqspark.util.Constant.{RawGenotype => CRG, InnerGenotype => CIG}
import org.dizhang.seqspark.util.UserConfig.{Variants, ImExConfig}

/**
  * hold the distributed genetype data, RDD[Variant]
  */

object VCF {

  def save(geno: VCF, path: String): Unit = {

    if (geno.config.gtOnly) {
      geno.vars.saveAsObjectFile(geno.config.path)
    } else {
      geno.rawVars.saveAsObjectFile(geno.config.path)
    }

  }

  def load(config: ImExConfig, sc: SparkContext): VCF = {
    if (config.gtOnly) {
      val data = sc.objectFile(config.path).asInstanceOf[RDD[Variant[Byte]]]
      ByteGenotype(data, config)
    } else {
      val data = sc.objectFile(config.path).asInstanceOf[RDD[Variant[String]]]
      StringGenotype(data, config)
    }
  }

}

sealed trait VCF {
  def config: ImExConfig
  def rawVars: RDD[Variant[String]]
  def vars: RDD[Variant[Byte]]
  def filter(regions: IntervalTree[Region]): VCF
  def filter(regions: Broadcast[IntervalTree[Region]]): VCF
  def select(indicator: Array[Boolean]): VCF
  def select(indicator: Broadcast[Array[Boolean]]): VCF
  def toDummy: VCF
}

case class StringGenotype(rawVars: RDD[Variant[String]], config: ImExConfig) extends VCF {
  def vars = rawVars.map(v => v.map(g => g.bt))
  def filter(regions: IntervalTree[Region]): StringGenotype = {
     StringGenotype(rawVars.filter(v => IntervalTree.overlap(regions, v.toRegion)), config)
  }
  def filter(regions: Broadcast[IntervalTree[Region]]): StringGenotype = {
    StringGenotype(rawVars.filter(v => IntervalTree.overlap(regions.value, v.toRegion)), config)
  }
  def select(indicator: Array[Boolean]): StringGenotype = {
    StringGenotype(rawVars.map(v => v.select(indicator)), config)
  }
  def select(indicator: Broadcast[Array[Boolean]]): StringGenotype = {
    StringGenotype(rawVars.map(v => v.select(indicator.value)), config)
  }
  def toDummy = StringGenotype(rawVars.map(v => v.toDummy), config)
}

case class ByteGenotype(vars: RDD[Variant[Byte]], config: ImExConfig) extends VCF {
  def rawVars =
    if (config.phased) {
      vars.map(v => v.map(g => g.toUnPhased))
    } else {
      vars.map(v => v.map(g => g.toPhased))
    }
  def filter(regions: IntervalTree[Region]): ByteGenotype = {
    ByteGenotype(vars.filter(v => IntervalTree.overlap(regions, v.toRegion)), config)
  }
  def filter(regions: Broadcast[IntervalTree[Region]]): ByteGenotype = {
    ByteGenotype(vars.filter(v => IntervalTree.overlap(regions.value, v.toRegion)), config)
  }
  def select(indicator: Array[Boolean]): ByteGenotype = {
    ByteGenotype(vars.map(v => v.select(indicator)), config)
  }
  def select(indicator: Broadcast[Array[Boolean]]): ByteGenotype = {
    ByteGenotype(vars.map(v => v.select(indicator.value)), config)
  }
  def toDummy = ByteGenotype(vars.map(v => v.toDummy), config)
}
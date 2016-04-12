package org.dizhang.seqspark.ds

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.{IntervalTree, Regions}
import org.dizhang.seqspark.util.Constant.{InnerGenotype => CIG, RawGenotype => CRG}
import org.dizhang.seqspark.util.UserConfig.{ImExConfig, ImExType, Variants}

/**
  * hold the distributed genetype data, RDD[Variant]
  */

object VCF {

  def save(geno: VCF, path: String): Unit = {
    val exCnf = geno.config
    def makeGT(b: Byte): String = {
      if (exCnf.phased) b.toPhased else b.toUnPhased
    }
    (exCnf.format, exCnf.gtOnly) match {
      case (ImExType.cache, true) => geno.vars.saveAsObjectFile(exCnf.path)
      case (ImExType.cache, false) => geno.rawVars.saveAsObjectFile(exCnf.path)
      case (ImExType.vcf, true) => geno.vars.map(v => v.toString(makeGT(_))).saveAsTextFile(exCnf.path)
      case (_, _) => geno.rawVars.map(v => v.toString(s => s)).saveAsTextFile(exCnf.path)
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
  var config: ImExConfig
  def rawVars: RDD[Variant[String]]
  def vars: RDD[Variant[Byte]]
  def filter(regions: Regions): VCF
  def filter(regions: Broadcast[Regions]): VCF
  def select(indicator: Array[Boolean]): VCF
  def select(indicator: Broadcast[Array[Boolean]]): VCF
  def toDummy: VCF
  def updateConfig(cnf: ImExConfig): Unit = {
    config = cnf
  }
}

case class StringGenotype(rawVars: RDD[Variant[String]], var config: ImExConfig) extends VCF {
  def vars = rawVars.map(v => v.map(g => g.bt))
  def filter(regions: Regions): StringGenotype = {
     StringGenotype(rawVars.filter(v => regions.overlap(v.toRegion)), config)
  }
  def filter(regions: Broadcast[Regions]): StringGenotype = {
    StringGenotype(rawVars.filter(v => regions.value.overlap(v.toRegion)), config)
  }
  def select(indicator: Array[Boolean]): StringGenotype = {
    StringGenotype(rawVars.map(v => v.select(indicator)), config)
  }
  def select(indicator: Broadcast[Array[Boolean]]): StringGenotype = {
    StringGenotype(rawVars.map(v => v.select(indicator.value)), config)
  }
  def toDummy = StringGenotype(rawVars.map(v => v.toDummy), config)
}

case class ByteGenotype(vars: RDD[Variant[Byte]], var config: ImExConfig) extends VCF {
  def rawVars =
    if (config.phased) {
      vars.map(v => v.map(g => g.toUnPhased))
    } else {
      vars.map(v => v.map(g => g.toPhased))
    }
  def filter(regions: Regions): ByteGenotype = {
    ByteGenotype(vars.filter(v => regions.overlap(v.toRegion)), config)
  }
  def filter(regions: Broadcast[Regions]): ByteGenotype = {
    ByteGenotype(vars.filter(v => regions.value.overlap(v.toRegion)), config)
  }
  def select(indicator: Array[Boolean]): ByteGenotype = {
    ByteGenotype(vars.map(v => v.select(indicator)), config)
  }
  def select(indicator: Broadcast[Array[Boolean]]): ByteGenotype = {
    ByteGenotype(vars.map(v => v.select(indicator.value)), config)
  }
  def toDummy = ByteGenotype(vars.map(v => v.toDummy), config)
}
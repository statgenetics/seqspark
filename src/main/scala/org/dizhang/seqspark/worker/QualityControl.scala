package org.dizhang.seqspark.worker

import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.annot.linkVariantDB
import org.slf4j.LoggerFactory
import Genotypes._
import Variants._
import Samples._

/**
  * Created by zhangdi on 9/25/16.
  */
object QualityControl {
  val logger = LoggerFactory.getLogger(getClass)
  def cleanVCF(input: Data[String])(implicit ssc: SingleStudyContext): Data[Byte] = {
    logger.info("start quality control")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated =  linkVariantDB(decompose(input))(conf, sc)

    annotated.persist(StorageLevel.MEMORY_AND_DISK)

    statGdGq(annotated)(ssc)

    val simpleVCF: Data[Byte] = toSimpleVCF(genotypeQC(annotated, conf.qualityControl.genotypes))

    simpleVCF.persist(StorageLevel.MEMORY_AND_DISK)
    /** sample QC */
    checkSex(simpleVCF)(ssc)

    titv(simpleVCF)(ssc)

    /** Variant QC */
    simpleVCF.variants(conf.qualityControl.variants)(ssc)
    annotated.unpersist()
    simpleVCF.unpersist()
  }

  def cleanImputed(input: Data[(Double, Double, Double)])(implicit ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated = linkVariantDB(input)(conf, sc)
    annotated.persist(StorageLevel.MEMORY_AND_DISK)

    /** sample QC */
    checkSex(annotated)(ssc)

    /** Variant QC */
    annotated.variants(conf.qualityControl.variants)(ssc)
  }
}

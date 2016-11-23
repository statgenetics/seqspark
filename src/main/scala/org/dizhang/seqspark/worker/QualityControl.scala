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

    annotated.cache()
    annotated.checkpoint()
    annotated.foreach(_ => Unit)

    val sums = ssc.userConfig.qualityControl.summaries

    if (sums.contains("annotation")) {
      countByFunction(annotated)
    }

    if (sums.contains("gdgq")) {
      statGdGq(annotated)(ssc)
    }

    val cleaned = genotypeQC(annotated, conf.qualityControl.genotypes)

    val simpleVCF: Data[Byte] = toSimpleVCF(cleaned)

    simpleVCF.cache()
    simpleVCF.checkpoint()
    simpleVCF.foreach(_ => Unit)

    //simpleVCF.checkpoint()
    //simpleVCF.persist(StorageLevel.MEMORY_AND_DISK)
    /** sample QC */

    if (sums.contains("sexCheck")) {
      checkSex(simpleVCF)(ssc)
    }

    if (sums.contains("titv")) {
      titv(simpleVCF)(ssc)
    }

    if (sums.contains("pca")) {
      pca(simpleVCF)(ssc)
    }

    /** Variant QC */
    simpleVCF.variants(conf.qualityControl.variants)(ssc)
    //annotated.unpersist()
    //simpleVCF.unpersist()
  }

  def cleanImputed(input: Data[(Double, Double, Double)])(implicit ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated = linkVariantDB(input)(conf, sc)
    annotated.cache()

    //annotated.checkpoint()

    val sums = ssc.userConfig.qualityControl.summaries
    /** sample QC */
    if (sums.contains("sexCheck")) {
      checkSex(annotated)(ssc)
    }

    if (sums.contains("pca")) {
      pca(annotated)(ssc)
    }

    /** Variant QC */
    annotated.variants(conf.qualityControl.variants)(ssc)
  }
}

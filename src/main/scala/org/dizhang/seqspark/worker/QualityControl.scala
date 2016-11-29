package org.dizhang.seqspark.worker

import org.dizhang.seqspark.annot.linkVariantDB
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.worker.Genotypes._
import org.dizhang.seqspark.worker.Samples._
import org.dizhang.seqspark.worker.Variants._
import org.slf4j.LoggerFactory

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

    //annotated.cache()
    val sums = ssc.userConfig.qualityControl.summaries
    if (sums.contains("gdgq")) {
      annotated.checkpoint()
      statGdGq(annotated)(ssc)
      if (conf.benchmark) {
        annotated.foreach(_ => Unit)
        logger.info("annotated data ready")
      }
    }


    if (sums.contains("annotation")) {
      countByFunction(annotated)
    }



    val cleaned = genotypeQC(annotated, conf.qualityControl.genotypes)

    val simpleVCF: Data[Byte] = toSimpleVCF(cleaned)

    simpleVCF.cache()
    simpleVCF.checkpoint()
    if (conf.benchmark) {
      simpleVCF.foreach(_ => Unit)
      logger.info("genotype QC completed")
    }

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

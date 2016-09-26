package org.dizhang.seqspark.worker

import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.geno.GeneralizedVCF._
import org.dizhang.seqspark.annot.linkVariantDB

/**
  * Created by zhangdi on 9/25/16.
  */
object QualityControl {
  def cleanVCF(input: Data[String])(implicit ssc: SingleStudyContext): Data[Byte] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated =  linkVariantDB(input.decompose())(conf, sc).cache()
    annotated.statGdGq(ssc)
    val simpleVCF: Data[Byte] = annotated.genotypeQC(conf.qualityControl.genotypes).toSimpleVCF.cache()

    /** sample QC */
    simpleVCF.checkSex(ssc)

    /** Variant QC */
    simpleVCF.variantsFilter(conf.qualityControl.variants)(ssc)
  }

  def cleanImputed(input: Data[(Double, Double, Double)])(implicit ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated = linkVariantDB(input)(conf, sc).cache()

    /** sample QC */
    annotated.checkSex(ssc)

    /** Variant QC */
    annotated.variantsFilter(conf.qualityControl.variants)(ssc)
  }
}

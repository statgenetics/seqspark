/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.worker

import org.dizhang.seqspark.annot.{linkVariantDB, linkGeneDB}
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.worker.Genotypes._
import org.dizhang.seqspark.worker.Samples._
import org.dizhang.seqspark.worker.Variants._
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by zhangdi on 9/25/16.
  */

sealed trait QualityControl[A, B] {
  def clean(input: Data[A])(implicit ssc: SingleStudyContext): Data[B]
}

object QualityControl {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  type Imp = (Double, Double, Double)

  implicit object VCF extends QualityControl[String, Byte] {
    def clean(input: Data[String])(implicit ssc: SingleStudyContext): Data[Byte] = {
      cleanVCF(input)
    }
  }

  implicit object Imputed extends QualityControl[Imp, Imp] {
    def clean(input: Data[Imp])(implicit ssc: SingleStudyContext): Data[Imp] = {
      cleanImputed(input)
    }
  }

  def cleanVCF(input: Data[String])(implicit ssc: SingleStudyContext): Data[Byte] = {
    logger.info("start quality control")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val annotated = linkVariantDB(decompose(input))(conf, sc)

    //annotated.cache()
    val sums = ssc.userConfig.qualityControl.summaries
    if (sums.contains("gdgq")) {
      annotated.checkpoint()
      if (conf.benchmark) {
        //annotated.foreach(_ => Unit)
        logger.info(s"annotated data ready: ${annotated.count()} variants")
      }
      statGdGq(annotated)(ssc)
    }


    val cleaned = genotypeQC(annotated, conf.qualityControl.genotypes)

    val simpleVCF: Data[Byte] = toSimpleVCF(cleaned)

    simpleVCF.cache()
    simpleVCF.checkpoint()
    if (conf.benchmark) {
      logger.info(s"genotype QC completed: ${simpleVCF.count()} variants")
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
    val res = simpleVCF.variants(conf.qualityControl.variants)(ssc)
    //annotated.unpersist()
    //simpleVCF.unpersist()

    val geneAnnot = if (sums.contains("annotation")) {
      logger.info("start gene annotation")
      val tmp = linkGeneDB(res)(conf, sc)
      logger.info("finished gene annotation")
      countByFunction(tmp)
      tmp
    } else {
      res
    }

    if (conf.qualityControl.save) {
      geneAnnot.cache()
      geneAnnot.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }
    if (conf.qualityControl.export) {
      geneAnnot.cache()
      geneAnnot.saveAsTextFile(conf.input.genotype.path + s".${conf.project}.vcf")
    }



    geneAnnot
  }

  def cleanImputed(input: Data[(Double, Double, Double)])
                  (implicit ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
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
    val res = annotated.variants(conf.qualityControl.variants)(ssc)
    if (conf.qualityControl.save) {
      res.cache()
      res.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }
    if (conf.qualityControl.export) {
      res.cache()
      res.saveAsTextFile(conf.input.genotype.path + s".${conf.project}.vcf")
    }
    res
  }
}

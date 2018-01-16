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

import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.util.SeqContext
import org.dizhang.seqspark.worker.Genotypes._
import org.dizhang.seqspark.worker.Samples._
import org.dizhang.seqspark.worker.Variants._
import org.slf4j.{LoggerFactory, Logger}

/**
  * Created by zhangdi on 9/25/16.
  */

sealed trait QualityControl[A, B] {
  def clean(input: Data[A], pass: Boolean)(implicit ssc: SeqContext): Data[B]
}

object QualityControl {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  type Imp = (Double, Double, Double)

  def apply[A, B](data: Data[A], a: A, b: B)
                                 (implicit ssc: SeqContext, qc: QualityControl[A, B]): Data[B] = {
    val pass = ! ssc.userConfig.pipeline.contains("qualityControl")

    qc.clean(data, pass)
  }

  implicit object CleanVCF extends QualityControl[Byte, Byte] {
    def clean(input: Data[Byte], pass: Boolean)(implicit ssc: SeqContext): Data[Byte] = {
      input
    }
  }

  implicit object VCF extends QualityControl[String, Byte] {
    def clean(input: Data[String], pass: Boolean)(implicit ssc: SeqContext): Data[Byte] = {
      if (pass) {
        Genotypes.toSimpleVCF(input)
      } else {
        cleanVCF(input)
      }
    }
  }

  implicit object Imputed extends QualityControl[Imp, Imp] {
    def clean(input: Data[Imp], pass: Boolean)(implicit ssc: SeqContext): Data[Imp] = {
      if (pass) {
        input
      } else {
        cleanImputed(input)
      }
    }
  }

  def cleanVCF(input: Data[String])(implicit ssc: SeqContext): Data[Byte] = {
    logger.info("start quality control")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    //val annotated = linkVariantDB(decompose(input))(conf, sc)

    //annotated.cache()
    val sums = ssc.userConfig.qualityControl.summaries
    if (sums.contains("gdgq")) {
      //input.checkpoint()
      //if (conf.benchmark) {
        //annotated.foreach(_ => Unit)
        //logger.info(s"raw data: ${input.count()} variants")
      //}
      statGdGq(input)(ssc)
    }

    /** 1. Genotype level QC */
    val cleaned = genotypeQC(input, conf.qualityControl.genotypes)
    //if (conf.benchmark) {
      //cleaned.cache()
      //logger.info(s"genotype QC completed: ${cleaned.count()} variants")
    //}
    /** 2. decompose */
    val decomposed = if (conf.input.genotype.decompose)
      decompose(cleaned)
    else
      cleaned
    //if (conf.benchmark) {
      //decomposed.cache()
      //logger.info(s"${decomposed.count()} variants after decomposition")
    //}

    /** 3. convert to Byte genotype */
    val simpleVCF: Data[Byte] = toSimpleVCF(decomposed)

    /** 4. impute missing genotype */
    val imputed: Data[Byte] = imputeMis(simpleVCF)(conf)


    /** 6. Variant level QC */
    val res = imputed.variants(conf.qualityControl.variants)(ssc)

    if (conf.benchmark) {
      res.cache()
      logger.info(s"${res.count()} variants after variant level QC")
    }


    res.cache()
    //res.checkpoint()

    //simpleVCF.checkpoint()
    //simpleVCF.persist(StorageLevel.MEMORY_AND_DISK)
    /** sample QC */

    if (sums.contains("sexCheck")) {
      checkSex(res)(ssc)
    }

    if (sums.contains("titv")) {
      titv(res)(ssc)
    }

    if (sums.contains("pca")) {
      pca(res)(ssc)
    }

    res
  }

  def cleanImputed(input: Data[(Double, Double, Double)])
                  (implicit ssc: SeqContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext

    //annotated.checkpoint()

    val sums = ssc.userConfig.qualityControl.summaries
    /** sample QC */
    if (sums.contains("sexCheck")) {
      checkSex(input)(ssc)
    }

    if (sums.contains("pca")) {
      pca(input)(ssc)
    }

    /** Variant QC */
    input.variants(conf.qualityControl.variants)(ssc)

  }
}

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
import org.dizhang.seqspark.util.Constant.Variant.InfoKey
import org.dizhang.seqspark.util.SeqContext
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhangdi on 9/25/16.
  */

sealed trait QualityControl[A, B] {
  def clean(input: Data[A], pass: Boolean)(implicit ssc: SeqContext): Data[B]
}

object QualityControl {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  type Imp = (Double, Double, Double)

  def apply[A, B](data: Data[A], a: A, b: B, pass: Boolean = false)
                 (implicit ssc: SeqContext, qc: QualityControl[A, B]): Data[B] = {

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
      Genotypes.statGdGq(input)(ssc)
    }

    if (sums.contains("titv")) {
      val ratio = Variants.titv(input)
      logger.info(s"titv ratio before QC: ${ratio._1.toDouble/ratio._2} = ${ratio._1}/${ratio._2}")
    }


    /** 1. Genotype level QC */
    val (cleaned, rawGenoCnts) = Genotypes.genotypeQC(input, conf.qualityControl.genotypes)(sc)
    //if (conf.benchmark) {
      //cleaned.cache()
      //logger.info(s"genotype QC completed: ${cleaned.count()} variants")
    //}
    //if (conf.benchmark) {
      //decomposed.cache()
      //logger.info(s"${decomposed.count()} variants after decomposition")
    //}

    if (conf.benchmark) {
      logger.info(s"${cleaned.count()} variants before QC")
      logger.info(s"${cleaned.map(v => v.parseInfo("SS_RawGeno").toInt).reduce((a, b) => a + b)} genotypes before QC")
    }


    /** 3. convert to Byte genotype */
    val simpleVCF: Data[Byte] = Genotypes.toSimpleVCF(cleaned)

    /** 4. Variant level QC */
    val res = simpleVCF.variants(conf.qualityControl.variants)(ssc)

    if (conf.benchmark) {
      res.cache()
      logger.info(s"${res.count()} variants after variant level QC")
      logger.info(s"${res.map(v => v.parseInfo("SS_CleanGeno").toInt).reduce((a, b) => a + b)} genotypes after QC")
      val maf = res.map{v =>
        val info = v.parseInfo
        if (info.contains(InfoKey.maf)) {
          Some(info(InfoKey.maf).toDouble)
        } else {
          None
        }
      }
      val rare = maf.map(m => m.map(d => d < 0.01 || d > 0.99))
      logger.info(s"${rare.filter(r => r.isDefined && r.get).count()} rare variants by annotation")
      logger.info(s"${rare.filter(r => r.isEmpty).count()} unknown variants by annotation")
      logger.info(s"${rare.filter(r => r.isDefined && ! r.get).count()} common variants by annotation")
    }

    /** 5. impute missing genotype */
    val imputed: Data[Byte] = Genotypes.imputeMis(res)(conf)

    res.cache()
    //res.checkpoint()

    //simpleVCF.checkpoint()
    //simpleVCF.persist(StorageLevel.MEMORY_AND_DISK)
    /** sample QC */

    if (sums.contains("sexCheck")) {
      Samples.checkSex(res)(ssc)
    }

    if (sums.contains("titv")) {
      val ratio = Variants.titv(res)
      logger.info(s"titv ratio after QC: ${ratio._1.toDouble/ratio._2} = ${ratio._1}/${ratio._2}")
    }

    if (sums.contains("pca")) {
      Samples.pca(res)(ssc)
    }

    if (conf.qualityControl.save) {
      res.cache()
      res.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }

    imputed
  }

  def cleanImputed(input: Data[(Double, Double, Double)])
                  (implicit ssc: SeqContext): Data[(Double, Double, Double)] = {
    val conf = ssc.userConfig
    val sc = ssc.sparkContext

    //annotated.checkpoint()

    val sums = ssc.userConfig.qualityControl.summaries
    /** sample QC */
    if (sums.contains("sexCheck")) {
      Samples.checkSex(input)(ssc)
    }

    if (sums.contains("pca")) {
      Samples.pca(input)(ssc)
    }

    /** Variant QC */
    val res = input.variants(conf.qualityControl.variants)(ssc)

    if (conf.qualityControl.save) {
      res.cache()
      res.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }

    res
  }
}

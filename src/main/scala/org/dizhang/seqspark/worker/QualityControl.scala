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

import java.io.PrintWriter

import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.util.Constant.Variant.InfoKey
import org.dizhang.seqspark.util.{LogicalParser, SeqContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

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

  def appendMsg(msg: mutable.Map[String, List[String]], key: String, value: String): Unit = {
    if (msg.contains(key)) {
      msg(key) = value :: msg(key)
    } else {
      msg(key) = List(value)
    }
  }

  def cleanVCF(input: Data[String])(implicit ssc: SeqContext): Data[Byte] = {
    logger.info("start quality control")

    val qc: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
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
      val msg = s"titv ratio before QC: ${ratio._1.toDouble / ratio._2} = ${ratio._1}/${ratio._2}"
      logger.info(msg)
      appendMsg(qc, "titv", s"beforeQC: ${ratio._1.toDouble / ratio._2}")
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

    //if (conf.benchmark) {



    /** 3. convert to Byte genotype */
    val simpleVCF: Data[Byte] = Genotypes.toSimpleVCF(cleaned)

    val markPass: Data[Byte] =
      simpleVCF.variants(conf.qualityControl.variants, Some("SS_PASS"), filter = false)(ssc)

    markPass.persist(StorageLevel.MEMORY_ONLY_SER)

    {
      val varCnt = simpleVCF.count()
      val varMsg = s"$varCnt variants before QC"
      logger.info(varMsg)
      appendMsg(qc, "variants", s"beforeQC: $varCnt")

      if (conf.qualityControl.genotypes != LogicalParser.T) {
        val genoCnt = simpleVCF.map(v => v.parseInfo("SS_RawGeno").toInt).reduce((a, b) => a + b)
        val genoMsg = s"$genoCnt genotypes before QC"
        logger.info(genoMsg)
        appendMsg(qc, "genotypes", s"beforeQC: $genoCnt")
      }
    }//}

    /** 4. Variant level QC */
    val res = markPass.variants(LogicalParser.parse("SS_PASS"))(ssc)

    //if (conf.benchmark) {
      //res.persist(StorageLevel.MEMORY_ONLY_SER)

    {
      val varCnt = res.count()
      logger.info(s"$varCnt variants after variant level QC")
      val maf = res.map { v =>
        val info = v.parseInfo
        if (info.contains(InfoKey.maf)) {
          Some(info(InfoKey.maf).toDouble)
        } else {
          None
        }
      }
      val rare = maf.map(m => m.map(d => d < 0.01 || d > 0.99))
      val rareCnt = rare.filter(r => r.isDefined && r.get).count()
      val unKnownCnt = rare.filter(r => r.isEmpty).count()
      val commonCnt = rare.filter(r => r.isDefined && !r.get).count()
      logger.info(s"$rareCnt rare variants by annotation")
      logger.info(s"$unKnownCnt unknown variants by annotation")
      logger.info(s"$commonCnt common variants by annotation")

      appendMsg(qc, "variants", s"afterQC: $varCnt")
      appendMsg(qc, "variants", s"afterQC(rare): $rareCnt")
      appendMsg(qc, "variants", s"afterQC(common): $commonCnt")

      if  (conf.qualityControl.genotypes != LogicalParser.T) {
        val genoCnt = if (res.isEmpty()) 0 else res.map(v => v.parseInfo("SS_CleanGeno").toInt).reduce((a, b) => a + b)
        logger.info(s"$genoCnt genotypes after QC")
        appendMsg(qc, "genotypes", s"afterQC: $genoCnt")
      }
    }
      //}



    /** 5. impute missing genotype */
    val imputed: Data[Byte] = Genotypes.imputeMis(res)(conf)

    //res.checkpoint()

    //simpleVCF.checkpoint()
    //simpleVCF.persist(StorageLevel.MEMORY_AND_DISK)
    /** sample QC */

    if (sums.contains("sexCheck")) {
      Samples.checkSex(res)(ssc)
    }

    if (sums.contains("titv")) {
      val ratio = Variants.titv(res)
      appendMsg(qc, "titv", s"afterQC: ${ratio._1.toDouble/ratio._2}")
      logger.info(s"titv ratio after QC: ${ratio._1.toDouble/ratio._2} = ${ratio._1}/${ratio._2}")
      if (conf.qualityControl.titvBy.contains("samples")) {
        Samples.titv(res)(ssc)
      }
    }

    if (sums.contains("pca")) {
      Samples.pca(res)(ssc)
    }

    if (conf.qualityControl.save) {
      res.cache()
      res.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }
    {
      val pw = new PrintWriter(conf.output.results.resolve("qc.txt").toFile)
      qc.foreach{
        case (k, v) =>
          pw.write(s"$k\n")
          v.reverse.foreach(l => pw.write(s"\t$l\n"))
      }
      pw.close()
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
    val res = input.variants(conf.qualityControl.variants, None, true)(ssc)

    if (conf.qualityControl.save) {
      res.cache()
      res.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }

    res
  }
}

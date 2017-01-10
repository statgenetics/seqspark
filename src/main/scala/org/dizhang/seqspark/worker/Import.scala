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

import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Phenotype, Region, Variant}
import org.dizhang.seqspark.util.LogicalParser.LogExpr
import org.dizhang.seqspark.util.{LogicalParser, SingleStudyContext, UserConfig => UC}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 8/13/16.
  */
sealed trait Import[A <: Import.Source, B] {
  def load(ssc: SingleStudyContext): Data[B]
}

object Import {

  sealed trait Source
  case object PlainVCF extends Source
  case object CacheVCF extends Source
  case object Impute2 extends Source
  case object CachedImpute2 extends Source


  val logger = LoggerFactory.getLogger(getClass)

  type Imp = (Double, Double, Double)

  implicit object RawVCF extends Import[PlainVCF.type , String] {
    def load(ssc: SingleStudyContext): Data[String] = {
      fromVCF(ssc)
    }
  }

  implicit object CachedVCF extends Import[CacheVCF.type , Byte] {
    def load(ssc: SingleStudyContext): Data[Byte] = {
      val path = ssc.userConfig.input.genotype.path + ".cache"
      ssc.sparkContext.objectFile(path, ssc.userConfig.jobs).asInstanceOf[Data[Byte]]
    }
  }

  implicit object Imputed extends Import[Impute2.type , Imp] {
    def load(ssc: SingleStudyContext): Data[Imp] = {
      fromImpute2(ssc)
    }
  }

  implicit object CachedImputed extends Import[CachedImpute2.type , Imp] {
    def load(ssc: SingleStudyContext): Data[Imp] = {
      val path = ssc.userConfig.input.genotype.path + ".cache"
      ssc.sparkContext.objectFile(path, ssc.userConfig.jobs).asInstanceOf[Data[Imp]]
    }
  }

  def fromVCF(ssc: SingleStudyContext): Data[String] = {

    def pass(l: String)
            (logExpr: LogExpr,
             names: Set[String],
             regions: Option[Regions] = None): Boolean = {
      /** This function "pass"
        * check if this variant should be included
        * before parse the whole line
        * */
      val isNotComment: Boolean = !l.startsWith("#")
      isNotComment && {
        if (logExpr == LogicalParser.T) {
          true
        } else {
          /** grab the first 8 fields using regex,
            * split will be slow when the line is very long
            * */
          val metaMatcher = """^([^\t]*\t){7}[^\t]*""".r
          val meta: Array[String] = metaMatcher.findFirstIn(l).get.split("\t")
          /** prepare the var map for the logical expression */
          val vmf = Map("FILTER" -> List(meta(6))) //the FORMAT field
          val vmi = if (names.exists(p => p.startsWith("INFO."))) {
            Variant.parseInfo(meta(7)).map {
              case (k, v) =>
                s"INFO.$k" -> v.split(",").toList
            }
          } else {
            Map.empty[String, List[String]]
          } //the info filed
          val in = if (regions.isEmpty) {
            /** none is yes, no need to do the interval tree search */
            true
          } else {
            val start = meta(1).toInt
            val end = start + meta(4).split(",").map(_.length).max
            val r = Region(meta(0), start, end)
            regions.get.overlap(r)
          }
          LogicalParser.evalExists(logExpr)(vmf ++ vmi) && in
        }
      }


    }
    logger.info("start import ...")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = Phenotype("phenotype")(ssc.sparkSession)

    val imConf = conf.input.genotype
    val noSample = imConf.samples match {
      case Left(UC.Samples.none) => true
      case _ => false
    }
    val filter = imConf.filter
    val terms = LogicalParser.names(filter)
    val raw = sc.textFile(imConf.path, conf.jobs)
    val default = "0/0"
    /** prepare a regions tree to filter variants */
    val regions = sc.broadcast(imConf.variants match {
      case Left(UC.Variants.all) =>
        logger.info("using all variants")
        None
      case Left(UC.Variants.exome) =>
        logger.info("using variants on exome")
        //val coord = conf.annotation.RefSeq.getString("coord")
        //val exome = sc.broadcast(Regions.makeExome(coord)(sc))
        val coord = conf.annotation.RefSeq.getString("coord")
        Some(Regions.makeExome(coord)(sc))
      case Left(_) =>
        logger.info("using all variants")
        None
      case Right(tree) =>
        logger.info(s"using user specified regions")
        Some(tree)
    })
    /** filter variants based on meta information
      * before making the actual genotype data for each sample
      * */
    val s1 = raw.filter{l =>
      pass(l)(filter, terms, regions.value)
    }.map(l => Variant.fromString(l, default, noSample = noSample))
    //logger.info(s"total variants: ${s1.count()} in ${imConf.path}")
    //logger.info(s"imported variants: ${s2.count()}")
    /** now filter unwanted samples if specified */
    val s3 = if (noSample) {
      s1
    } else {
      imConf.samples match {
        case Left(_) => s1
        case Right(s) =>
          val samples = pheno.indicate(s)
          Phenotype.select(s, "phenotype")
          s1.samples(samples)(sc)
      }
    }
    s3
  }

  def fromImpute2(ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    logger.info("start import ...")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    val imConf = conf.input
    val imputedFile = imConf.genotype.path
    val imputedInfoFile = imConf.genotype.path + "_info"
    val default = (1.0, 0.0, 0.0)
    val imputedGeno = sc.textFile(imputedFile, conf.jobs).map{l =>
      val v = Variant.fromImpute2(l, default)
      (v.toRegion, v)
    }
    val imputedInfo = sc.textFile(imputedInfoFile).map{l =>
      val s = l.split("\\s+")
      (Region(s(0), s(2).toInt), s(4))
    }
    val imputed = imputedGeno.leftOuterJoin(imputedInfo)
    val res = imputed.map{
      case (r, (v, Some(i))) =>
        v.addInfo("IMS", i)
        v
      case (r, (v, None)) =>
        v
    }
    //logger.info(s"imported variants: ${res.count()}")
    //res.unpersist()
    res
  }
}

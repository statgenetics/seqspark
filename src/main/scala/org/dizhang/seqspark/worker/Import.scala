package org.dizhang.seqspark.worker

import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.{Region, Variant}
import org.dizhang.seqspark.geno.GeneralizedVCF._
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.util.{UserConfig => UC}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 8/13/16.
  */

object Import {

  val logger = LoggerFactory.getLogger(getClass)

  def fromVCF(ssc: SingleStudyContext): Data[String] = {
    logger.info("start import ...")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = ssc.phenotype
    val imConf = conf.input.genotype
    val noSample = imConf.samples match {
      case Left(UC.Samples.none) => true
      case _ => false
    }
    val raw = sc.textFile(imConf.path)
    val default = "0/0"
    val s1 = raw filter (l => ! l.startsWith("#") ) map (l => Variant.fromString(l, default, noSample = noSample))
    //s1.cache()
    //logger.info(s"total variants: ${s1.count()} in ${imConf.path}")
    val s2 = imConf.variants match {
      case Left(UC.Variants.all) => s1
      case Left(UC.Variants.exome) =>
        val coord = conf.annotation.RefSeq.getString("coord")
        val exome = sc.broadcast(Regions.makeExome(coord)(sc))
        s1 filter (v => exome.value.overlap(v.toRegion))
      case Left(_) => s1
      case Right(tree) => s1 filter (v => tree.overlap(v.toRegion))
    }
    //s2.cache()
    //s1.unpersist()
    //logger.info(s"imported variants: ${s2.count()}")
    val s3 = if (noSample) {
      s2
    } else {
      imConf.samples match {
        case Left(_) => s2
        case Right(s) =>
          val samples = pheno.indicate(s)
          s2.samples(samples)(sc)
      }
    }
    //s2.unpersist()
    s3
  }

  def fromImpute2(ssc: SingleStudyContext): Data[(Double, Double, Double)] = {
    logger.info("start import ...")
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = ssc.phenotype
    val imConf = conf.input
    val imputedFile = imConf.genotype.path
    val imputedInfoFile = imConf.genotype.path + "_info"
    val default = (1.0, 0.0, 0.0)
    val imputedGeno = sc.textFile(imputedFile).map{l =>
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

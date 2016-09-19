package org.dizhang.seqspark.geno

import org.apache.spark.SparkContext
import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.{Region, Variant}
import org.dizhang.seqspark.util.Constant.UnPhased
import org.dizhang.seqspark.util.{SeqContext, SingleStudyContext, UserConfig}
import org.dizhang.seqspark.util.UserConfig.{ImExConfig, RootConfig, Samples, Variants}
import org.dizhang.seqspark.util.Constant._
import org.dizhang.seqspark.geno.GeneralizedVCF._

import scalaz.Reader

/**
  * Created by zhangdi on 8/13/16.
  */
object Import {

  def fromVCF = Reader[SingleStudyContext, Data[String]]{ssc =>
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = ssc.phenotype
    val imConf = conf.`import`
    val noSample = imConf.samples match {
      case Left(Samples.none) => true
      case _ => false
    }
    val raw = sc.textFile(imConf.path)
    val default = if (! imConf.phased) UnPhased.Gt.ref else UnPhased.Gt.ref.bt.toPhased
    val s1 = raw filter (l => ! l.startsWith("#") ) map (l => Variant.fromString(l, default, noSample = noSample))
    val s2 = imConf.variants match {
      case Left(Variants.all) => s1
      case Left(Variants.exome) => {
        val coord = conf.annotation.geneCoord
        val exome = sc.broadcast(Regions.makeExome(coord))
        s1 filter (v => exome.value.overlap(v.toRegion))
      }
      case Left(_) => s1
      case Right(tree) => s1 filter (v => tree.overlap(v.toRegion))
    }
    val s3 = if (noSample) {
      s2
    } else {
      imConf.samples match {
        case Left(Samples.all) => s2
        case Right(s) =>
          val samples = pheno.indicate(s)
          s2.samples(samples)(sc)
      }
    }
    s3
  }
  def fromImpute2 = Reader[SingleStudyContext, Data[(Double, Double, Double)]]{ssc =>
    val conf = ssc.userConfig
    val sc = ssc.sparkContext
    val pheno = ssc.phenotype
    val imConf = conf.`import`
    val imputedFile = imConf.path
    val imputedInfoFile = imConf.path + "_info"
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
    imputed.map{
      case (r, (v, Some(i))) =>
        v.addInfo("IMS", i)
        v
      case (r, (v, None)) =>
        v
    }
  }
}

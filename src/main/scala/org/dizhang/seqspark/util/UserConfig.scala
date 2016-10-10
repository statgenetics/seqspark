package org.dizhang.seqspark.util

import com.typesafe.config.{Config, ConfigFactory}
import org.dizhang.seqspark.ds.Region
import org.dizhang.seqspark.annot._

import collection.JavaConverters._
import scala.io.Source

/**
  * Wrapper around the typesafe.config
  *
  * If anything of the config file changes,
  * no need to dig into the source code elsewhere
  */
object UserConfig {

  object GenomeBuild extends Enumeration {
    val hg18 = Value("hg18")
    val hg19 = Value("hg19")
    val hg38 = Value("hg38")
  }

  object ImportGenotypeType extends Enumeration {
    val vcf = Value("vcf")
    val imputed = Value("imputed")
    val cache = Value("cache")
  }

  object Samples extends Enumeration {
    val all = Value("all")
    val none = Value("none")
  }

  object Variants extends Enumeration {
    val all = Value("all")
    val exome = Value("exome")
    val none = Value("none")
  }

  object MutType extends Enumeration {
    val snv = Value("snv")
    val indel = Value("indel")
    val cnv = Value("cnv")
  }

  object MethodType extends Enumeration {
    val skat = Value("skat")
    val skato = Value("skato")
    val meta = Value("meta")
    val cmc = Value("cmc")
    val brv = Value("brv")
    val snv = Value("snv")
  }

  object WeightMethod extends Enumeration {
    val none = Value("none")
    val equal = Value("equal")
    val wss = Value("wss")
    val erec = Value("erec")
    val skat = Value("skat")
    val annotation = Value("annotation")
  }

  object TestMethod extends Enumeration {
    val score = Value("score")
    val lhr = Value("lhr")
    val wald = Value("wald")
  }

  case class RootConfig(config: Config) extends UserConfig {

    def project = config.getString("project")
    def localDir = config.getString("localDir")
    def hdfsDir = config.getString("hdfsDir")
    def pipeline = config.getStringList("pipeline").asScala.toList

    def qualityControl = QualityControlConfig(config.getConfig("qualityControl"))

    def input = InputConfig(config.getConfig("input"))
    def annotation = AnnotationConfig(config.getConfig("annotation"))
    def association = AssociationConfig(config.getConfig("association"))
  }

  case class InputConfig(config: Config) extends UserConfig {
    def genotype = GenotypeConfig(config.getConfig("genotype"))
    def phenotype = PhenotypeConfig(config.getConfig("phenotype"))
  }

  case class GenotypeConfig(config: Config) extends UserConfig {

    def format = ImportGenotypeType.withName(config.getString("format"))


    def path = config.getString("path")


    def filters = config.getStringList("filters").asScala.toArray

    def genomeBuild = GenomeBuild.withName(config.getString("genomeBuild"))

    def samples: Either[Samples.Value, String] = {
      config.getString("samples") match {
        case "all" => Left(Samples.all)
        case "none" => Left(Samples.none)
        case field => Right(field)
      }
    }

    def variants: Either[Variants.Value, Regions] = {
      val file = """file://(.+)""".r
      config.getString("variants") match {
        case "all" => Left(Variants.all)
        case "exome" => Left(Variants.exome)
        case file(f) => Right(Regions(Source.fromFile(f).getLines().map(Region(_))))
        case x => Right(Regions(x.split(",").map(Region(_)).toIterator))
      }
    }
  }

  case class PhenotypeConfig(config: Config) extends UserConfig {
    def path = config.getString("path")
    def batch = config.getString("batch")
  }

  case class QualityControlConfig(config: Config) extends UserConfig {
    def genotypes = config.getStringList("genotypes").asScala.toList
    def variants = config.getStringList("variants").asScala.toList
  }

  case class AnnotationConfig(config: Config) extends UserConfig {
    def dbDir = config.getString("dbDir")
    def variants = config.getConfig("variants")
    def genes = config.getConfig("genes")
    def RefSeq = config.getConfig("RefSeq")
    def dbNSFP = config.getConfig("dbNSFP")
    def CADD = config.getConfig("CADD")
    def ExAC = config.getConfig("ExAC")
  }

  case class AssociationConfig(config: Config) extends UserConfig {
    def traitList = config.getStringList("trait.list").asScala.toArray
    def methodList = config.getStringList("method.list").asScala.toArray
    def method(name: String) = MethodConfig(config.getConfig(s"method.$name"))
    def `trait`(name: String) = TraitConfig(config.getConfig(s"trait.$name"))
    def filters = config.getStringList("filters").asScala.toArray
  }

  case class MethodConfig(config: Config) extends UserConfig {
    def `type` = MethodType.withName(config.getString("type"))
    def weight = WeightMethod.withName(config.getString("weight"))
    def maf = config.getConfig("maf")
    def resampling = if (config.hasPath("resampling")) config.getBoolean("resampling") else false
    def test = TestMethod.withName("score")
    def misc: Config = {
      if (config.hasPath("misc")) config.getConfig("misc") else ConfigFactory.empty()
    }
  }

  case class TraitConfig(config: Config) extends UserConfig {
    def binary = config.getBoolean("binary")
    def covariates = if (config.hasPath("covariates")) {
      config.getStringList("covariates").asScala.toArray
    } else {
      Array[String]()
    }

    def pc: Int = if (config.hasPath("pc")) {
      config.getInt("pc")
    } else {
      0
    }

    def conditional = if (config.hasPath("conditional")) {
      config.getStringList("conditional").asScala.toArray
    } else {
      Array[String]()
    }
  }

  case class MetaConfig(config: Config) extends UserConfig {
    def project = config.getString("project")
    def localDir = config.getString("localDir")
    def hdfsDir = config.getString("hdfsDir")
    def studies = config.getStringList("studies").asScala.toArray
    def traitList = config.getStringList("trait.list").asScala.toArray
    def methodList = config.getStringList("method.list").asScala.toArray
    def `trait`(name: String) = TraitConfig(config.getConfig(s"trait.$name"))
    def method(name: String) = MetaConfig(config.getConfig(s"method.$name"))
  }

}

sealed trait UserConfig {
  def config: Config
}

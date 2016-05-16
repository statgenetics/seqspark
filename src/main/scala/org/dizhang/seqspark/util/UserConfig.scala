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

  object ImExType extends Enumeration {
    val vcf = Value("vcf")
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
    val meta = Value("meta")
    val cmc = Value("cmc")
    val brv = Value("burden")
    val single = Value("single")
  }

  object WeightMethod extends Enumeration {
    val none = Value("none")
    val equal = Value("equal")
    val wss = Value("wss")
    val erec = Value("erec")
    val skat = Value("skat")
    val annotation = Value("annotation")
  }

  object MafSource extends Enumeration {
    val pooled = Value("pooled")
    val controls = Value("controls")
    val annotation = Value("annotation")
  }

  object TestMethod extends Enumeration {
    val score = Value("score")
    val lhr = Value("lhr")
    val wald = Value("wald")
  }

  case class RootConfig(config: Config) extends UserConfig {

    def project = config.getString("project")
    def seqSparkHome = config.getString("seqSparkHome")
    def localDir = config.getString("localDir")
    def hdfsDir = config.getString("hdfsDir")
    def pipeline = config.getStringList("pipeline").asScala.toList

    def `import` = ImExConfig(config.getConfig("import"))
    def export = ImExConfig(config.getConfig("export"))
    def genotypeLevelQC = GenotypeLevelQCConfig(config.getConfig("genotypeLevelQC"))
    def sampleLevelQC = SampleLevelQCConfig(config.getConfig("sampleLevelQC"))
    def variantLevelQC = VariantLevelQCConfig(config.getConfig("variantLevelQC"))
    def annotation = AnnotationConfig(config.getConfig("annotation"))
    def association = AssociationConfig(config.getConfig("association"))
    def meta = MetaConfig(config.getConfig("meta"))
  }

  case class ImExConfig(config: Config) extends UserConfig {

    def format = ImExType.withName(config.getString("format"))

    def gtOnly = config.getBoolean("gtOnly")

    def phased = config.getBoolean("phased")

    def path = config.getString("path")

    def sampleInfo = config.getString("sampleInfo")

    def filters = config.getStringList("filters").asScala.toArray

    def genomeBuild = GenomeBuild.withName(config.getString("genomeBuild"))

    def maxAlleleNum = config.getInt("maxAlleleNum")

    def mutType: Array[MutType.Value] = {
      config.getStringList("mutType").asScala.map(m => MutType.withName(m)).toArray
    }

    def save = config.getBoolean("save")

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

  case class GenotypeLevelQCConfig(config: Config) extends UserConfig {

    def format = config.getString("format").split(":").zipWithIndex.toMap
    def gd = config.getIntList("gd").asScala.toArray
    def gq = config.getInt("gq")
    def save = config.getBoolean("save")

  }

  case class SampleLevelQCConfig(config: Config) extends UserConfig {
    def pcaMaf = config.getDouble("pcaMaf")
    def missing = config.getDouble("missing")
    def save = config.getBoolean("save")
  }

  case class VariantLevelQCConfig(config: Config) extends UserConfig {
    def missing = config.getDouble("missing")
    def batchMissing = config.getDouble("batchMissing")
    def batchSpec = config.getInt("batchSpec")
    def autosome = config.getBoolean("autosome")
    def hwe = config.getDouble("hwe")
    def save = config.getBoolean("save")
  }

  case class AnnotationConfig(config: Config) extends UserConfig {
    def mafPath = config.getString("maf.path")
    def mafTag = config.getString("maf.tag")
    def mafAN = config.getString("maf.an")
    def mafAC = config.getString("maf.ac")
    def geneBuild = config.getString("gene.build")
    def geneCoord = config.getString("gene.coord")
    def geneSeq = config.getString("gene.seq")
    def dbNSFP = config.getString("dbNSFP")
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
    def mafSource = MafSource.withName(config.getString("maf.source"))
    def mafCutoff = config.getDouble("maf.cutoff")
    def mafFixed = config.getBoolean("maf.fixed")
    def resampling = if (config.hasPath("resampling")) config.getBoolean("resampling") else false
    def test = TestMethod.withName("score")
    def misc: Config = {
      if (config.hasPath("misc")) config.getConfig("misc") else ConfigFactory.empty()
    }
  }

  case class TraitConfig(config: Config) extends UserConfig {
    def binary = config.getBoolean("binary")
    def covariates = config.getStringList("covariates").asScala.toArray
  }

  case class MetaConfig(config: Config) extends UserConfig {
    def studies = config.getStringList("studies").asScala.toArray
  }

}

sealed trait UserConfig {
  def config: Config
}

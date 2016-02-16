package org.dizhang.seqspark.util

import com.typesafe.config.Config
import UserConfig.ConfigValue._
import org.dizhang.seqspark.ds.Region
import org.dizhang.seqspark.annot._
import collection.JavaConverters._
import scala.io.Source

/**
  * Created by zhangdi on 2/15/16.
  */
object UserConfig {

  object ConfigValue {
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
    }

    object CodingMethod extends Enumeration {
      val cmc = Value("cmc")
      val brv = Value("burden")
      val single = Value("single")
    }

    object WeightMethod extends Enumeration {
      val none = Value("none")
      val equal = Value("equal")
      val wss = Value("wss")
      val erec = Value("erec")
      val annotation = Value("annotation")
    }

    object MafSource extends Enumeration {
      val pooled = Value("pooled")
      val controls = Value("controls")
      val annotation = Value("annotation")
    }

  }

}

sealed trait UserConfig {
  def config: Config
}

case class RootConfig(config: Config) extends UserConfig {

  def `import` = ImExConfig(config.getConfig("import"))
  def export = ImExConfig(config.getConfig("export"))
  def sampleInfo = SampleInfoConfig(config.getConfig("sampleInfo"))
  def genotypeLevelQC = GenotypeLevelQCConfig(config.getConfig("genotypeLevelQC"))
  def sampleLevelQC = SampleLevelQCConfig(config.getConfig("sampleLevelQC"))
  def variantLevelQC = VariantLevelQCConfig(config.getConfig("variantLevelQC"))
  def annotation = AnnotationConfig(config.getConfig("annotation"))
  def association = AssociationConfig(config.getConfig("association"))
}

case class ImExConfig(config: Config) extends UserConfig {

  def `type` = ImExType.withName(config.getString("type"))

  def phased = config.getBoolean("phased")

  def path = config.getString("path")

  def filters = config.getStringList("filters").asScala.toArray

  def genomeBuild = GenomeBuild.withName(config.getString("genomeBuild"))

  def maxAlleles = config.getInt("maxAlleles")

  def snv = config.getBoolean("snv")

  def indel = config.getBoolean("indel")

  def save = config.getBoolean("save")

  def samples: Either[Samples.Value, Set[String]] = {
    val file = """file://(.+)""".r
    config.getString("samples") match {
      case "all" => Left(Samples.all)
      case "none" => Left(Samples.none)
      case file(f) => Right(Source.fromFile(f).getLines().toSet)
      case x => Right(x.split(",").toSet)
    }
  }

  def variants: Either[Variants.Value, IntervalTree[Region]] = {
    val file = """file://(.+)""".r
    config.getString("variants") match {
      case "all" => Left(Variants.all)
      case file(f) => Right(IntervalTree(Source.fromFile(f).getLines().map(Region(_))))
      case x => Right(IntervalTree(x.split(",").map(Region(_)).toIterator))
    }
  }

}

case class SampleInfoConfig(config: Config) extends UserConfig {
  def source = config.getString("source")
  def batch = config.getString("batch")
  def filter = config.getString("filter")
  def control = config.getString("config")
  def sex = config.getString("sex")
}

case class GenotypeLevelQCConfig(config: Config) extends UserConfig {

  def format = config.getString("format")
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
}

case class AssociationConfig(config: Config) extends UserConfig {
  def traitList = config.getStringList("trait.list").asScala.toArray
  def methodList = config.getStringList("method.list").asScala.toArray
  def method(name: String) = MethodConfig(config.getConfig(s"method.$name"))
  def `trait`(name: String) = TraitConfig(config.getConfig(s"trait.$name"))
  def filters = config.getStringList("filters").asScala.toArray
}

case class MethodConfig(config: Config) extends UserConfig {
  def coding = CodingMethod.withName(config.getString("coding"))
  def weight = WeightMethod.withName(config.getString("weight"))
  def mafSource = MafSource.withName(config.getString("maf.source"))
  def mafCutoff = config.getDouble("maf.cutoff")
  def mafFixed = config.getBoolean("maf.fixed")
  def resampling = config.getBoolean("resampling")
}

case class TraitConfig(config: Config) extends UserConfig {
  def binary = config.getBoolean("binary")
  def covariates = config.getStringList("covariates").asScala.toArray
}

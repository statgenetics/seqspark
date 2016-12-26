package org.dizhang.seqspark.util

import com.typesafe.config.{Config, ConfigFactory}
import org.dizhang.seqspark.annot._
import org.dizhang.seqspark.ds.Region
import org.dizhang.seqspark.util.LogicalParser.LogExpr
import scala.collection.JavaConverters._
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
    val imputed = Value("impute2")
    val cachevcf = Value("cachedVcf")
    val cacheimputed = Value("cachedImpute2")
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

    val project = config.getString("project")
    val localDir = config.getString("localDir")
    val outDir = localDir + "/output"
    val dbDir = config.getString("dbDir")
    val pipeline = config.getStringList("pipeline").asScala.toList
    val jobs = config.getInt("jobs")
    val benchmark = config.getBoolean("benchmark")
    val debug = config.getBoolean("debug")

    val qualityControl = QualityControlConfig(config.getConfig("qualityControl"))

    val input = InputConfig(config.getConfig("input"))
    val annotation = AnnotationConfig(config.getConfig("annotation"))
    val association = AssociationConfig(config.getConfig("association"))
  }

  case class InputConfig(config: Config) extends UserConfig {
    val genotype = GenotypeConfig(config.getConfig("genotype"))
    val phenotype = PhenotypeConfig(config.getConfig("phenotype"))
  }

  case class GenotypeConfig(config: Config) extends UserConfig {

    val format = ImportGenotypeType.withName(config.getString("format"))

    val path = config.getString("path")


    //val filters = config.getStringList("filters").asScala.toArray

    val filter: LogExpr = LogicalParser.parse(config.getStringList("filter").asScala.toList)

    //val genomeBuild = GenomeBuild.withName(config.getString("genomeBuild"))

    val samples: Either[Samples.Value, String] = {
      config.getString("samples") match {
        case "all" => Left(Samples.all)
        case "none" => Left(Samples.none)
        case field => Right(field)
      }
    }

    val variants: Either[Variants.Value, Regions] = {
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
    val path = config.getString("path")
    val batch = config.getString("batch")
  }

  case class QualityControlConfig(config: Config) extends UserConfig {
    val genotypes: LogExpr = LogicalParser.parse(config.getStringList("genotypes").asScala.toList)
    val variants: LogExpr = LogicalParser.parse(config.getStringList("variants").asScala.toList)
    val summaries = config.getStringList("summaries").asScala.toList
    val save = config.getBoolean("save")
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
    def sites: String = config.getString("method.sites")

  }

  case class MiscConfig(config: Config) extends UserConfig {
    val varLimit: Int = {
      if (config.hasPath("varLimit"))
        config.getInt("varLimit")
      else
        500
    }
    val groupBy: List[String] = {
      if (config.hasPath("groupBy"))
        config.getStringList("groupBy").asScala.toList
      else
        List("gene")
    }
    val variants: List[String] = {
      if (config.hasPath("variants"))
        config.getStringList("variants").asScala.toList
      else
        List("isFunctional")
    }
    val method: String = {
      if (config.hasPath("method"))
        config.getString("method")
      else
        "liu.mod"
    }
    val rho: Double = {
      if (config.hasPath("rho"))
        config.getDouble("rho")
      else
        0.0
    }
    val rhos: Array[Double] = {
      if (config.hasPath("rhos"))
        config.getDoubleList("rhos").asScala.toArray.map(_.toDouble)
      else
        Array[Double]()
    }
    val kernel: String = {
      if (config.hasPath("kernel"))
        config.getString("kernel")
      else
        "linear.weighted"
    }
    val weightBete: List[Double] = {
      if (config.hasPath("weightBeta"))
        config.getDoubleList("weightBeta").asScala.toList.map(_.toDouble)
      else
        List(1.0, 25.0)
    }
    val smallSampleAdjustment: Boolean = {
      if (config.hasPath("smallSampleAdjustment"))
        config.getBoolean("smallSampleAdjustment")
      else
        true
    }
  }

  case class MethodConfig(config: Config) extends UserConfig {
    val `type` = MethodType.withName(config.getString("type"))
    val weight = WeightMethod.withName(config.getString("weight"))
    val maf = config.getConfig("maf")
    val resampling = if (config.hasPath("resampling")) config.getBoolean("resampling") else false
    val test = TestMethod.withName("score")
    val misc: MiscConfig = MiscConfig(config.getConfig("misc"))
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
    def dbDir = config.getString("dbDir")
    def studies = config.getStringList("studies").asScala.toArray
    def traitList = config.getStringList("trait.list").asScala.toArray
    def methodList = config.getStringList("method.list").asScala.toArray
    def `trait`(name: String) = TraitConfig(config.getConfig(s"trait.$name"))
    def method(name: String) = MetaConfig(config.getConfig(s"method.$name"))
  }

}

sealed trait UserConfig extends Serializable {

  @transient def config: Config
}

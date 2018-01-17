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

package org.dizhang.seqspark.util

import com.typesafe.config.Config
import org.dizhang.seqspark.annot._
import org.dizhang.seqspark.ds.Region
import org.dizhang.seqspark.util.LogicalParser.LogExpr

import scala.collection.JavaConverters._
import scala.io.Source
import java.nio.file.{Files, Path, Paths}

import org.apache.hadoop
import org.apache.hadoop.fs
import org.slf4j.{Logger, LoggerFactory}

/**
  * Wrapper around the typesafe.config
  *
  * If anything of the config file changes,
  * no need to dig into the source code elsewhere
  */
object UserConfig {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy val hadoopConf = new hadoop.conf.Configuration()
  lazy val hdfs = hadoop.fs.FileSystem.get(hadoopConf)

  def getProperPath(unchecked: String): String = {
    if (unchecked.isEmpty) {
      //logger.warn(s"using empty path")
      unchecked
    } else if (unchecked.startsWith("file:")) {
      unchecked
    } else if (hdfs.exists(new fs.Path(unchecked))) {
      unchecked
    } else if (Files.exists(Paths.get(unchecked))) {
      "file://" + Paths.get(unchecked).toAbsolutePath.toString
    } else {
      logger.error(s"path doesn't exist on HDFS or Local FS: $unchecked")
      ""
    }
  }

  object GenomeBuild extends Enumeration {
    val hg18 = Value("hg18")
    val hg19 = Value("hg19")
    val hg38 = Value("hg38")
  }

  object GenotypeFormat extends Enumeration {
    val vcf = Value("vcf")
    val imputed = Value("impute2")
    //val bgen = Value("bgen")
    //val cacheFullvcf = Value("cachedFullVcf")
    val cacheVcf = Value("cachedVcf")
    val cacheImputed = Value("cachedImpute2")
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
    //val lhr = Value("lhr")
    val wald = Value("wald")
  }

  object ImputeMethod extends Enumeration {
    val bestGuess = Value("bestGuess")
    val meanDosage = Value("meanDosage")
    val random = Value("random")
    val no = Value("no")
  }

  object DBFormat extends Enumeration {
    val vcf: Value = Value("vcf")
    val plain: Value = Value("plain")
    val csv: Value = Value("csv")
    val tsv: Value = Value("tsv")
  }

  case class RootConfig(config: Config) extends UserConfig {

    val project = config.getString("project")
    val localDir = config.getString("localDir")

    def local: Boolean = config.hasPath("local") && config.getBoolean("local")

    val outDir = localDir + "/output"
    val dbDir = config.getString("dbDir")
    val pipeline = config.getStringList("pipeline").asScala.toList
    val partitions = config.getInt("partitions")
    val benchmark = config.getBoolean("benchmark")
    val debug = config.getBoolean("debug")
    val cache = config.getBoolean("cache")
    val output: OutputConfig = OutputConfig(config.getConfig("output"), project)

    val qualityControl = QualityControlConfig(config.getConfig("qualityControl"))

    val input = InputConfig(config.getConfig("input"))
    val annotation = AnnotationConfig(config.getConfig("annotation"))
    val association = AssociationConfig(config.getConfig("association"))
    val meta = MetaConfig(config.getConfig("meta"))
  }

  case class InputConfig(config: Config) extends UserConfig {
    val genotype = GenotypeConfig(config.getConfig("genotype"))
    val phenotype = PhenotypeConfig(config.getConfig("phenotype"))
  }

  case class OutputConfig(config: Config, proj: String) extends UserConfig {
    val genotype = GenotypeConfig(config.getConfig("genotype"))
    val results: Path = {
      if (config.hasPath("results"))
        Paths.get(config.getString("results")).toAbsolutePath
      else
        Paths.get(proj).toAbsolutePath
    }
  }

  case class GenotypeConfig(config: Config) extends UserConfig {

    def format = GenotypeFormat.withName(config.getString("format"))

    def pathRaw: String = config.getString("path")

    def path = getProperPath(pathRaw)

    def pathValid = path != ""

    //val filters = config.getStringList("filters").asScala.toArray

    val filters: LogExpr = LogicalParser.parse(config.getStringList("filters").asScala.toList)

    val export: Boolean = if (config.hasPath("export")) config.getBoolean("export") else false

    val supersede: Boolean = if (config.hasPath("supersede")) config.getBoolean("supersede") else false

    def save: Boolean = if (config.hasPath("save")) config.getBoolean("save") else false

    def cache: Boolean = if (config.hasPath("cache")) config.getBoolean("cache") else false

    val decompose: Boolean = if (config.hasPath("decompose")) config.getBoolean("decompose") else false
    //val genomeBuild = GenomeBuild.withName(config.getString("genomeBuild"))

    val impute: ImputeMethod.Value = ImputeMethod.withName(config.getString("impute"))

    val missing: ImputeMethod.Value = ImputeMethod.withName(config.getString("missing"))

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
    def pathRaw: String = config.getString("path")
    def path = getProperPath(pathRaw)
    def pathValid = path != ""
    val batch = config.getString("batch")
  }

  case class QualityControlConfig(config: Config) extends UserConfig {
    val genotypes: LogExpr = LogicalParser.parse(config.getStringList("genotypes").asScala.toList)
    val variants: LogExpr = LogicalParser.parse(config.getStringList("variants").asScala.toList)
    val summaries = config.getStringList("summaries").asScala.toList
    val save = config.getBoolean("save")
    val export = config.getBoolean("export")
    val pca = PCAConfig(config.getConfig("pca"))
    def groups: List[String] = {
      if (config.hasPath("groups"))
        config.getStringList("groups").asScala.toList
      else
        List[String]()
    }
  }

  case class PCAConfig(config: Config) extends UserConfig {
    def variants: LogExpr = LogicalParser.parse(config.getStringList("variants").asScala.toList)
    def impute: String = config.getString("impute")
    def noprune: Boolean = config.getBoolean("noprune")
    def normalize: Boolean = config.getBoolean("normalize")
    def prune: Config = config.getConfig("prune")
  }

  case class AnnotationConfig(config: Config) extends UserConfig {
    def dbDir = config.getString("dbDir")
    def variants = config.getConfig("variants")
    def genes = config.getConfig("genes")
    def RefSeq = config.getConfig("db.RefSeq")
    def dbNSFP = config.getConfig("dbNSFP")
    def CADD = config.getConfig("CADD")
    def ExAC = config.getConfig("ExAC")
    def dbList: List[String] = config.getConfig("db").root().keySet().asScala.toList
    def db(name: String): DatabaseConfig = DatabaseConfig(config.getConfig(s"db.$name"))
    def addInfo: Map[String, String] = {
      val keys = config.getConfig("addInfo").root().keySet().asScala.toList
      keys.map(k => k -> config.getString(s"addInfo.$k")).toMap
    }
  }

  case class DatabaseConfig(config: Config) extends UserConfig {
    def format: DBFormat.Value = DBFormat.withName(config.getString("format"))
    def delimiter: String = format match {
      case DBFormat.vcf => ""
      case _ => config.getString("delimiter")
    }
    def header: Array[String] = format match {
      case DBFormat.vcf => Array[String]()
      case _ => config.getStringList("header").asScala.toArray
    }
    def pathRaw: String = config.getString("path")
    def path: String = if (config.hasPath("path")) getProperPath(pathRaw) else ""
    def pathValid: Boolean = path != ""
  }

  case class AssociationConfig(config: Config) extends UserConfig {
    def traitList = config.getStringList("trait.list").asScala.toArray
    def methodList = config.getStringList("method.list").asScala.toArray
    def method(name: String) = MethodConfig(config.getConfig(s"method.$name"))
    def `trait`(name: String) = TraitConfig(config.getConfig(s"trait.$name"))
    def sites: String = config.getString("method.sites")

  }

  case class MiscConfig(config: Config) extends UserConfig {
    val impute: ImputeMethod.Value = ImputeMethod.withName(config.getString("impute"))
    val varLimit: (Int, Int) = {
      if (config.hasPath("varLimit")) {
        val res = config.getIntList("varLimit").asScala.toList
        if (res.isEmpty) {
          (0, Int.MaxValue)
        } else {
          (res(0), res(1))
        }
      } else
        (0, 2000)
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
    val test = if (config.hasPath("test"))
      TestMethod.withName(config.getString("test"))
    else
      TestMethod.score
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
    def methodList = config.getStringList("method.list").asScala.toArray
    def method(name: String) = MethodConfig(config.getConfig(s"method.$name"))
    def studyList = config.getStringList("study.list").asScala.toArray
    def study(name: String) = StudyConfig(config.getConfig(s"study.$name"))
    def conditional = if (config.hasPath("conditional")) {
      config.getStringList("conditional").asScala.toArray
    } else {
      Array[String]()
    }
  }

  case class StudyConfig(config: Config) extends UserConfig {
    def pathRaw: String = config.getString("path")
    def path: String = getProperPath(pathRaw)
    def pathValid: Boolean = path != ""
  }

}

sealed trait UserConfig extends Serializable {

  @transient def config: Config
}

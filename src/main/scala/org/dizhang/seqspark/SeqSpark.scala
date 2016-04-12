package org.dizhang.seqspark

import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.ds.{Bed, Counter}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.worker.{GenotypeLevelQC, Import}
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

import org.slf4j.LoggerFactory
import worker._

import scala.collection.JavaConversions._

/**
 * Main function
 */

object SeqSpark {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    /** check args */
    if (!checkArgs(args)) {
      System.exit(1)
    }

    /** quick run */
    val userConfFile = new File(args(0))
    require(userConfFile.exists())
    try {

      implicit val userConf = ConfigFactory
        .parseFile(userConfFile)
        .withFallback(ConfigFactory.load().getConfig("seqa"))
        .resolve()

      //userConf.root().foreach{case (k, v) => println(s"$k: ${v.toString}")}

      implicit val rootConf = RootConfig(userConf)

      val modules = if (args.length == 2) args(1) else "1-4"
      checkConf
      run
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  def checkArgs(args: Array[String]): Boolean = {
    val usage = """
                |spark-submit --class SeqA [options] /path/to/wesqc.xx.xx.jar seqa.conf [modules]
                |
                |    options:     Spark options, e.g. --num-executors, please refer to the spark documentation.
                |
                |    seqa.conf:   The configuration file in INI format, could be other name.
                |
                |    modules:     The modules to run, default 1-4. Could be a single number (e.g. 3),
                |                 a range (e.g. 1-3), or comma separated list (e.g. 1,3,2,4).
                |
                |                 1: Genotype level QC, currently only hard filter
                |                     If you provide a vcf without extra format field other than GT,
                |                     do nothing. Otherwise, this module is highly recommended, because
                |                     even for VCF after GATK, genotypes with low DP or GQ could be very
                |                     inaccurate.
                |                 2: Sample level QC
                |                     Check sex, run PCA, filter samples with high missing rate, etc.
                |                 3: Variant level QC
                |                     Filter variants with high missing rate with experiment batch awareness;
                |                     filter batch-specific artifacts (variants other than singleton, and only
                |                      appear in one batch, and not recorded in a database previously)
                |                 4: Annotation
                |                     Annotate the variants using annovar.
                |                 5: Association test
                |                     Run rare variant association test
                |
                |"""

    if (args.length == 0 || args.length > 2) {
      println(usage)
      false
    } else if (args.length == 2) {
      val p1 = """([1-6])""".r
      val p2 = """([1-6])-([1-6])""".r
      val p3 = """([1-6])(,[1-6])+""".r
      args(1) match {
        case p1(s) => true
        case p2(s1, s2) if s2.toInt >= s1.toInt => true
        case p2(s1, s2) if s2.toInt < s1.toInt => println(usage); false
        case p3(_*) => true
        case _ => println(usage); false
      }
    } else
      true
  }

  def checkConf(implicit cnf: Config) {
    logger.info("Conf file fine")
  }

  def run(implicit cnf: RootConfig): Unit = {
    /**
     * May have other run mode later
     **/
    quickRun
  }

  /**
   * quick run. run through the specified modules
   * act as if the vcf is the only input
   */

  def quickRun(implicit cnf: RootConfig) {
    val project = cnf.project

    /** determine the input
    val dirs = List("genotypeLevelQC", "sampleLevelQC", "variantLevelQC", "annotation", "association")
    val rangeP = """(\d+)-(\d+)""".r
    val listP = """(\d+)(,\d+)+""".r
    val singleP = """(\d+)""".r
    val s = modules match {
      case rangeP(start, end) => (start.toInt to end.toInt).toList
      case listP(_*) => modules.split(",").map(_.toInt).toList
      case singleP(a) => List(a.toInt)
    }
    */
    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqA-%s" format project)
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[Bed], classOf[Var], classOf[Counter[Pair]]))
    implicit val sc: SparkContext = new SparkContext(scConf)

    val pipeline = cnf.pipeline

    val binder = org.slf4j.impl.StaticLoggerBinder.getSingleton
    logger.debug("hellor world")
    logger.debug(binder.getLoggerFactoryClassStr)

    logger.info(s"pipeline: ${(("import" :: pipeline) ::: List("export")).mkString(" ")}")

    val current = Import({})

    val last = Worker.recurSlaves(current, pipeline)

    Export(last)
    //PropertyConfigurator.configure("log4j.properties")
  }
}

package org.dizhang.seqspark

import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.ds.{Bed, Counter}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import org.slf4j.LoggerFactory
import worker._
import util.General._

/**
 * Main function
 */

object SingleStudy {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    /** check args */
    if (badArgs(args)) {
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


  def checkConf(implicit cnf: Config) {
    logger.info("Conf file fine")
  }

  def run(implicit cnf: RootConfig) {
    val project = cnf.project

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

    val last = WorkerObsolete.recurSlaves(current, pipeline)

    Export(last)
    //PropertyConfigurator.configure("log4j.properties")
  }
}

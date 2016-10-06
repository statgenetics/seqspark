package org.dizhang.seqspark

import java.io.File

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import org.dizhang.seqspark.util.UserConfig.RootConfig
import util.General._
/**
  * meta analysis
  */
object MetaAnalysis {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def meta(args: Array[String]): Unit = {

    if (badArgs(args)) {
      logger.error(s"bad arguments format: '${args.mkString(" ")}'")
      System.exit(1)
    }

    val userConfFile = new File(args(0))
    val userConf = ConfigFactory
      .parseFile(userConfFile)
      .withFallback(ConfigFactory.load().getConfig("meta"))
      .resolve()

    implicit val rootConf = RootConfig(userConf)
    run
  }

  def run(implicit rootConf: RootConfig): Unit = {
    logger.info("start meta analysis")
    logger.info("end meta analysis")
  }

}

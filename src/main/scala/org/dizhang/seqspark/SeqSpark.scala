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

package org.dizhang.seqspark

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.{Constant, QueryParser, SeqContext}
import org.dizhang.seqspark.util.UserConfig.{AnnotationConfig, RootConfig}
import org.dizhang.seqspark.worker.Annotation.logger
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 12/3/17.
  */
object SeqSpark {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {



    /** check args */
    if (badArgs(args)) {
      logger.error(s"bad argument: ${args.mkString(" ")}")
      System.exit(1)
    }

    val rootConf = readConf(args(0))


    val warehouse = if (rootConf.local)
      s"file://${Paths.get("").toAbsolutePath.toString}"
    else
      s"hdfs:///user/${System.getProperty("user.name")}"

    val ss: SparkSession = SparkSession
      .builder()
      .appName("SEQSpark")
      .config("spark.sql.warehouse", warehouse)
      .getOrCreate()

    val sc = ss.sparkContext

    val seqContext = SeqContext(rootConf, sc, ss)

    if (rootConf.pipeline.contains("meta")) {
      MetaAnalysis(seqContext)
    } else {
      SingleStudy(seqContext)
    }

  }
  def readConf(file: String): RootConfig = {
    val userConfFile = new File(file)
    require(userConfFile.exists())

    ConfigFactory.invalidateCaches()
    System.setProperty("config.file", file)
    val userConf = ConfigFactory.load().getConfig("seqspark")

    /**
    val userConf = ConfigFactory
      .parseFile(userConfFile)
      .withFallback(ConfigFactory.load().getConfig("seqspark"))
      .resolve()
      */
    val show = userConf.root().render()

    val rootConfig = RootConfig(userConf)

    if (rootConfig.debug) {
      logger.debug("Conf detail:\n" + show)
    }

    rootConfig
  }



}

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

import com.typesafe.config.ConfigFactory
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.slf4j.{Logger, LoggerFactory}
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
    main
  }

  def main(implicit rootConf: RootConfig): Unit = {
    logger.info("meta-analysis not available in this distribution, please update to the lastest version using 'git pull'")
    //logger.info("start meta analysis")
    //logger.info("end meta analysis")
  }

}

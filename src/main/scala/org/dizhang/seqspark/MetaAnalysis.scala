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

import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.util.SeqContext
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.meta._
import org.slf4j.{Logger, LoggerFactory}
/**
  * meta analysis
  */
object MetaAnalysis {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply(seqContext: SeqContext): Unit = {

    logger.info("start meta analysis")

    /** Spark configuration */

    val mm = new MetaMaster(seqContext)

    mm.run()

    logger.info("end meta analysis")
  }

}

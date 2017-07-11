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

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.ds.{Bed, Counter}
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.slf4j.{Logger, LoggerFactory}
/**
  * meta analysis
  */
object MetaAnalysis {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def meta(rootConf: RootConfig): Unit = {
    val user = System.getenv("USER")
    val hdfsHome = s"hdfs:///user/$user"

    val project = rootConf.project

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqSpark-%s" format project)
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //scConf.registerKryoClasses(Array(classOf[ConfigObject], classOf[Config], classOf[Bed], classOf[Var], classOf[Counter[(Double, Double)]]))
    val sc: SparkContext = new SparkContext(scConf)

    /** set checkpoint folder to hdfs home*/
    sc.setCheckpointDir(hdfsHome + "/checkpoint")


  }

  def main(args: Array[String]): Unit = {
    logger.info("meta-analysis not available in this distribution, please update to the lastest version using 'git pull'")

    val rootConf = SingleStudy.readConf(args(0))

    meta(rootConf)
    //logger.info("start meta analysis")
    //logger.info("end meta analysis")
  }

}

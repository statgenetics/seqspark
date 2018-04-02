/*
 * Copyright 2018 Zhang Di
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
import BaseSpecs._
import org.dizhang.seqspark.util.UserConfig.RootConfig

class SingleStudySpec extends IntegrationSpec with SharedTestData {

  private val confFile = getClass.getResource("/anno.conf").getPath
  private val conf = SeqSpark.readConf(confFile)

  def readConf(path: String): RootConfig = {
    val file = getClass.getResource(s"/$path").getPath
    SeqSpark.readConf(file)
  }

  def runPipeline(conf: RootConfig): Unit = {
    val ssc = util.SeqContext(conf, sc, spark)
    SingleStudy(ssc)
  }

  "A SingleStudy" should "read conf file properly" in {
    logger.info(s"Conf path: $confFile")
    val conf = SeqSpark.readConf(confFile)
    SingleStudy.checkConf(conf)
    logger.info(s"genotype path: ${conf.input.genotype.path}")
    logger.info(s"phenotype path: ${conf.input.phenotype.path}")

    conf.annotation.dbList.foreach(db =>
      logger.info(s"$db path: ${conf.annotation.db(db).path}")
    )
  }

  ignore should "load genotype into sc" in {
    val geno = sc.textFile(conf.input.genotype.path)
    logger.info(s"${geno.filter(! _.startsWith("#")).count()} variants")
  }

  ignore should "load phenotype into spark" in {
    val pheno = spark.read.options(ds.Phenotype.options).csv(conf.input.phenotype.path)
    logger.info(s"${pheno.count()} samples")
  }

  ignore should "run the whole thing" in {
    val ssc = util.SeqContext(conf, sc, spark)
    SingleStudy(ssc)
  }

  it should "run annotation" in {
    runPipeline(readConf("anno.conf"))
  }

  ignore should "run qc" in {
    runPipeline(readConf("qc.conf"))
  }

  ignore should "run association" in {
    runPipeline(readConf("assoc.conf"))
  }

}

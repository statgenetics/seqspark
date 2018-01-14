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

class SingleStudySpec extends IntegrationSpec with SharedTestData {

  val confFile = getClass.getResource("/demo.conf").getPath
  val conf = SeqSpark.readConf(confFile)


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

  it should "load genotype into sc" in {
    val geno = sc.textFile(conf.input.genotype.path)
    logger.info(s"${geno.filter(! _.startsWith("#")).count()} variants")
  }

  it should "load phenotype into spark" in {
    val pheno = spark.read.options(ds.Phenotype.options).csv(conf.input.phenotype.path)
    logger.info(s"${pheno.count()} samples")
  }

  it should "run the whole thing" in {

  }

}

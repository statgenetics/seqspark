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

package org.dizhang.seqspark.meta

import org.apache.spark.SparkContext
import org.dizhang.seqspark.assoc.RareMetalWorker
import org.dizhang.seqspark.ds.{SummaryStatistic, Variation}
import org.dizhang.seqspark.meta.MetaMaster._
import org.dizhang.seqspark.util.MetaAnalysisContext
import org.dizhang.seqspark.util.UserConfig.TraitConfig
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 6/13/16.
  */
class MetaMaster(metaContext: MetaAnalysisContext) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def metaConfig = metaContext.userConfig
  def sc = metaContext.sparkContext

  def run(): Unit = {
    val traits = metaConfig.traitList
    traits.foreach(t => t)
  }

  def runTrait(name: String): Unit = {
    val traitConfig = metaConfig.`trait`(name)
    val conditionals = traitConfig.conditional.map(x => Variation(x))
    val studies = metaConfig.studies
    require(studies.length > 1, "you need at least two studies to run meta analysis")
    var res = loadSummaryStatistic(name, traitConfig, studies.head, conditionals)(sc)
    studies.tail.foreach{ study =>
      val cur = loadSummaryStatistic(name, traitConfig, study, conditionals)(sc)
      res = res ++ cur
    }
  }
}

object MetaMaster {

  def loadSummaryStatistic(`trait`: String,
                           traitConfig: TraitConfig,
                           study: String,
                           conditionals: Array[Variation])(implicit sc: SparkContext): SummaryStatistic = {

    val path = s"$study/${`trait`}/rmw"
    if (traitConfig.binary) {
      val stats = sc.objectFile[RareMetalWorker.DefaultRMWResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.DefaultRMWResult])
      SummaryStatistic.DefaultStatistics(`trait`, stats)
    } else {
      val stats = sc.objectFile[RareMetalWorker.DefaultRMWResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.DefaultRMWResult])
      SummaryStatistic.DefaultStatistics(`trait`, stats)
    }
  }
}
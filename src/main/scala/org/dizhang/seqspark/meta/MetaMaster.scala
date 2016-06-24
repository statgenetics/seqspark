package org.dizhang.seqspark.meta

import org.apache.spark.SparkContext
import org.dizhang.seqspark.assoc.RareMetalWorker
import org.dizhang.seqspark.ds.{SummaryStatistic, Variation}
import org.dizhang.seqspark.util.UserConfig.{MetaConfig, TraitConfig}
import org.slf4j.LoggerFactory
import MetaMaster._

/**
  * Created by zhangdi on 6/13/16.
  */
class MetaMaster(metaConfig: MetaConfig) {
  val logger = LoggerFactory.getLogger(this.getClass)

  def run(): Unit = {
    val traits = metaConfig.traitList
    traits.foreach(t => t)
  }

  def runTrait(name: String): Unit = {
    val traitConfig = metaConfig.`trait`(name)
    val conditionals = traitConfig.conditional.map(x => Variation(x))
    val studies = metaConfig.studies
    require(studies.length > 1, "you need at least two studies to run meta analysis")
    var res = loadSummaryStatistic(name, traitConfig, studies.head, conditionals)
    studies.tail.foreach{ study =>
      val cur = loadSummaryStatistic(name, traitConfig, study, conditionals)
      res = res ++ cur
    }
  }
}

object MetaMaster {

  def loadSummaryStatistic(`trait`: String,
                           traitConfig: TraitConfig,
                           study: String,
                           conditionals: Array[Variation])(implicit sc: SparkContext): SummaryStatistic = {

    val path = s"$study/$`trait`/rmw"
    if (traitConfig.binary) {
      val stats = sc.objectFile[RareMetalWorker.DichotomousResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.DichotomousResult])
      SummaryStatistic.DichotomousStatistic(`trait`, stats)
    } else {
      val stats = sc.objectFile[RareMetalWorker.ContinuousResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.ContinuousResult])
      SummaryStatistic.ContinuousStatistic(`trait`, stats)
    }
  }
}
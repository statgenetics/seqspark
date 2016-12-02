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
      val stats = sc.objectFile[RareMetalWorker.DefaultResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.DefaultResult])
      SummaryStatistic.DefaultStatistics(`trait`, stats)
    } else {
      val stats = sc.objectFile[RareMetalWorker.DefaultResult](path)
        .map(r => r.conditional(conditionals).asInstanceOf[RareMetalWorker.DefaultResult])
      SummaryStatistic.DefaultStatistics(`trait`, stats)
    }
  }
}
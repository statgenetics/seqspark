package org.dizhang.seqspark.util

import org.apache.spark.SparkContext
import org.dizhang.seqspark.pheno.Phenotype
import org.dizhang.seqspark.util.UserConfig.{MetaConfig, RootConfig}

/**
  * Created by zhangdi on 8/11/16.
  */

sealed trait SeqContext {
  def userConfig: UserConfig
  def sparkContext: SparkContext
}

case class SingleStudyContext(userConfig: RootConfig,
                              sparkContext: SparkContext,
                              phenotype: Phenotype) extends SeqContext

case class MetaAnalysisContext(userConfig: MetaConfig,
                               sparkContext: SparkContext) extends SeqContext


object SeqContext {
  def apply(cnf: RootConfig, sc: SparkContext, sdb: Phenotype): SeqContext = {
    SingleStudyContext(cnf, sc, sdb)
  }

  def apply(cnf: MetaConfig, sc: SparkContext): SeqContext =
    MetaAnalysisContext(cnf, sc)
}

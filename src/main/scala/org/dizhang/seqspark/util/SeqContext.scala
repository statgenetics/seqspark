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
  def phenotype: Phenotype
}

case class SingleStudyContext(userConfig: RootConfig,
                              sparkContext: SparkContext,
                              phenotype: Phenotype) extends SeqContext

case class MetaAnalysisContext(userConfig: MetaConfig,
                               sparkContext: SparkContext,
                               phenotype: Phenotype) extends SeqContext


object SeqContext {
  def apply(cnf: UserConfig, sc: SparkContext, sdb: Phenotype): SeqContext = {
    cnf match {
      case c: RootConfig => SingleStudyContext(c, sc, sdb)
      case c: MetaConfig => MetaAnalysisContext(c, sc, sdb)
    }
  }
}

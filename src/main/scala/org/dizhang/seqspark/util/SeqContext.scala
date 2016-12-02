package org.dizhang.seqspark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
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
                              sparkSession: SparkSession) extends SeqContext

case class MetaAnalysisContext(userConfig: MetaConfig,
                               sparkContext: SparkContext) extends SeqContext


object SeqContext {
  def apply(cnf: RootConfig, sc: SparkContext, ss: SparkSession): SeqContext = {
    SingleStudyContext(cnf, sc, ss)
  }

  def apply(cnf: MetaConfig, sc: SparkContext): SeqContext =
    MetaAnalysisContext(cnf, sc)
}

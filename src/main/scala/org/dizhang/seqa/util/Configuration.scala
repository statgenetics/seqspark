package org.dizhang.seqa.util

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Configuration trait, holding several confs.
  */

trait Configuration {
  def config: Config
  def sc: SparkContext
  def sqlContext: SQLContext
}

case class PhenotypeConfiguration(config: Config,
                              sc: SparkContext,
                              sqlContext: SQLContext) extends Configuration {
  val source = config.getString("sampleInfo.source")
}
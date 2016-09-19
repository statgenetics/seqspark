package org.dizhang.seqspark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.{IntervalTree, Location, RefGene, dbNSFP}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.{Constant, SingleStudyContext}
import org.dizhang.seqspark.util.UserConfig.RootConfig

import scalaz._

/**
  * Created by zhangdi on 8/15/16.
  */
package object annot {

  def joinVariantDB[A](vcf: RDD[Variant[A]]): Reader[SingleStudyContext, RDD[Variant[A]]] = {
    Reader[SingleStudyContext, RDD[Variant[A]]]{scc =>
      val cnf = scc.userConfig
      val sc = scc.sparkContext

      vcf}
  }


}

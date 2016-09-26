package org.dizhang.seqspark.geno

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.{IntervalTree, Regions}
import org.dizhang.seqspark.ds.{Counter, Region, Variant}
import org.slf4j.{Logger, LoggerFactory}
import org.dizhang.seqspark.util.Constant._
import org.dizhang.seqspark.util.{SingleStudyContext, UserConfig}
import org.dizhang.seqspark.util.UserConfig.{GenomeBuild, RootConfig}
import GeneralizedVCF._
import scala.language.implicitConversions

/**
  * generalize VCF format
  */

trait GeneralizedVCF[A] {
  def self: RDD[Variant[A]]

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def regions(regions: Regions)(sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(regions)
    self.filter(v => bc.value.overlap(v.toRegion))
  }
  def samples(indicator: Array[Boolean])(sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(indicator)
    self.map(v => v.select(bc.value))
  }
  def toDummy: RDD[Variant[A]] = {
    self.map(v => v.toDummy)
  }

}

object GeneralizedVCF {

  /**
  implicit def toGeneralizedVCF[A](data: RDD[Variant[A]]): GeneralizedVCF[A] = new GeneralizedVCF[A] {
    override def vars: RDD[Variant[A]] = data
  }
  */


  implicit def toRawVCF(vars: RDD[Variant[String]]): RawVCF = {
    RawVCF(vars)
  }

  implicit def toSimpleVCF(vars: RDD[Variant[Byte]]): SimpleVCF = {
    SimpleVCF(vars)
  }

  implicit def toImputedVCF(vars: RDD[Variant[(Double, Double, Double)]]): ImputedVCF = {
    ImputedVCF(vars)
  }


}

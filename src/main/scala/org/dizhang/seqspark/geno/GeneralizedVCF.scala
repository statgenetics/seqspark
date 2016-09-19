package org.dizhang.seqspark.geno

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util._
import scala.language.implicitConversions
import scalaz._

/**
  * generalize VCF format
  */

trait GeneralizedVCF[A] {
  def self: RDD[Variant[A]]

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

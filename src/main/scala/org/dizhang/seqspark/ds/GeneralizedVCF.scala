package org.dizhang.seqspark.ds

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.Regions
import org.apache.spark.SparkContext
import scala.language.implicitConversions

/**
  * generalize VCF format
  */

trait GeneralizedVCF[A] {
  def vars: RDD[Variant[A]]
  def filter(regions: Regions)(implicit sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(regions)
    vars.filter(v => bc.value.overlap(v.toRegion))
  }
  def select(indicator: Array[Boolean])(implicit sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(indicator)
    vars.map(v => v.select(bc.value))
  }
  def toDummy: RDD[Variant[A]] = {
    vars.map(v => v.toDummy)
  }
}

object GeneralizedVCF {

  implicit def toGeneralizedVCF[A](data: RDD[Variant[A]]): GeneralizedVCF[A] = new GeneralizedVCF[A] {
    override def vars: RDD[Variant[A]] = data
  }

}

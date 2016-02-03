package org.dizhang.seqspark.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.InputOutput._
import org.slf4j.{LoggerFactory, Logger}

/**
 * Pipeline worker
 */
object Worker {
  type RawVar = Variant[String]
  type Var = Variant[Byte]
  type RawVCF = RDD[RawVar]
  type VCF = RDD[Var]
  type AnnoVCF = RDD[(String, (Constant.Annotation.Feature.Feature, Var))]



  val slaves = Map[String, Worker[VCF, VCF]](
    "sample" -> SampleLevelQC,
    "variant" -> VariantLevelQC,
    "association" -> Association
  )
  def recurSlaves(input: VCF, sl: List[String])(implicit cnf: Config, sc: SparkContext): VCF = {
    if (sl.tail == Nil)
      slaves(sl.head)(input)
    else
      recurSlaves(slaves(sl.head)(input), sl.tail)
  }
}

/** An abstract class that only contains a run method */
trait Worker[A, B] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val name: WorkerName
  def apply(input: A)(implicit cnf: Config, sc: SparkContext): B
}
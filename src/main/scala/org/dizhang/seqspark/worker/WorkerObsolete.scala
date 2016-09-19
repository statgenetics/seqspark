package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Phenotype, VCF, Variant}
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.slf4j.{LoggerFactory, Logger}
import scalaz._
/**
 * Pipeline worker
 */

object WorkerObsolete {
  //type RawVar = Variant[String]
  //type Var = Variant[Byte]
  //type RawVCF = RDD[RawVar]
  //type VCF = RDD[Var]
  type AnnoVCF = RDD[(String, (Constant.Annotation.Feature.Feature, Var))]
  type Data = (VCF, Phenotype)

  type Work[A] = Reader[SeqContext, (Phenotype, RDD[Variant[A]])]


  val slaves = Map[String, WorkerObsolete[Data, Data]](
    "genotype" -> GenotypeLevelQC,
    "sample" -> SampleLevelQC,
    "variant" -> VariantLevelQC,
    "variant" -> VariantLevelQC,
    "annotation" -> Annotation,
    "association" -> Association
  )


  def recurSlaves(input: Data, sl: List[String])(implicit cnf: RootConfig, sc: SparkContext): Data = {
    if (sl.isEmpty)
      input
    else
      recurSlaves(slaves(sl.head)(input), sl.tail)
  }
}

/** An abstract class that only contains a run method */
trait WorkerObsolete[A, B] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val name: WorkerName
  def apply(input: A)(implicit cnf: RootConfig, sc: SparkContext): B
}
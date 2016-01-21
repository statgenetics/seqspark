package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.dizhang.seqa.util.InputOutput._
import org.slf4j.{LoggerFactory, Logger}

/**
 * Pipeline worker
 */
object Worker {


  val slaves = Map[String, Worker[VCF, VCF]](
    "sample" -> SampleLevelQC,
    "variant" -> VariantLevelQC,
    "annotation" -> Annotation,
    "association" -> Association,
    "export" -> Export
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
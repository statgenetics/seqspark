package org.dizhang.seqa.worker
import org.ini4j.Ini
import org.apache.spark.SparkContext
import org.dizhang.seqa.util.InputOutput._

/**
 * Created by zhangdi on 8/18/15.
 */
object Worker {
  val slaves = Map[String, Worker[VCF, VCF]](
    "sample" -> SampleLevelQC,
    "variant" -> VariantLevelQC,
    "annotation" -> Annotation)

  def recurSlaves(input: VCF, sl: List[String])(implicit ini: Ini, sc: SparkContext): VCF = {
    if (sl.tail == Nil)
      slaves(sl.head)(input)
    else
      recurSlaves(slaves(sl.head)(input), sl.tail)
  }
}

/** An abstract class that only contains a run method*/
trait Worker[A, B] {
  val name: WorkerName
  def apply(input: A)(implicit ini: Ini, sc: SparkContext): B
}
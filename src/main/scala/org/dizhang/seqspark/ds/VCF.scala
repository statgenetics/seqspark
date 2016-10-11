package org.dizhang.seqspark.ds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.util.{LogicalExpression, SingleStudyContext}
import org.dizhang.seqspark.worker.Variants._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

/**
  * generalize VCF format
  */

@SerialVersionUID(102L)
class VCF[A: Genotype](self: RDD[Variant[A]]) extends Serializable {

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

  def variants(cond: List[String])(ssc: SingleStudyContext): RDD[Variant[A]] = {
    val conf = ssc.userConfig
    val pheno = ssc.phenotype
    val batch = pheno.batch(conf.input.phenotype.batch)
    val controls = pheno.select("control")
    val ctrlInd = if (controls.forall(c => c.isEmpty || c.get == "1")) {
      /** if everybody is either control or unknown, assume they are all controls */
      None
    } else {
      Some(controls.map{case Some("1") => true; case _ => false})
    }

    val myCond = cond.map(c => s"($c)").reduce((a,b) => s"$a and $b")
    logger.info(s"filter variants with '$myCond' ...")
    val names: Set[String] = LogicalExpression.analyze(myCond)

    self.filter{v =>
      val varMap = names.toArray.map{
        case "chr" => "chr" -> v.chr
        case "maf" => "maf" -> v.maf(ctrlInd).toString
        //case "batchMaf" => "batchMaf" -> v.batchMaf(ctrlInd, batch).values.min.toString
        case "missingRate" => "missingRate" -> (1 - v.callRate).toString
        case "batchMissingRate" => "batchMissingRate" -> (1 - v.batchCallRate(batch).values.min).toString
        case "alleleNum" => "alleleNum" -> v.alleleNum.toString
        case "batchSpecific" => "batchSpecific" -> v.batchSpecific(batch).values.max.toString
        case "hwePvalue" => "hwePvalue" -> v.hwePvalue(ctrlInd).toString
        case "isFunctional" => "isFunctional" -> v.isFunctional.toString
        case x => x -> v.parseInfo.getOrElse(x, "0")
      }.toMap
      LogicalExpression.judge(varMap)(myCond)
    }
  }

}

object VCF {

  implicit def toGeneralizedVCF[A: Genotype](data: RDD[Variant[A]]): VCF[A] = new VCF(data)


}

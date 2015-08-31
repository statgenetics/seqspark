package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.dizhang.seqa.util.Command
import org.dizhang.seqa.util.InputOutput._
import sys.process._

import scala.io.Source

/**
 * Created by zhangdi on 8/18/15.
 */

object Annotation extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("annotation")

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    val exitCode = ("mkdir -p %s" format workerDir).!
    println(exitCode)
    val rawSites = workerDir + "/sites.raw.vcf"
    val annotatedSites = "sites.annotated"
    writeRDD(input.map(_.meta.slice(0, 8).mkString("\t") + "\n"), rawSites)
    Command.annovar(rawSites, annotatedSites, workerDir)
    val annot = sc.broadcast(readAnnot(annotatedSites))
    input.zipWithIndex().map{
      case (v, i: Long) => {
      val meta = v.meta.clone()
      meta(7) =
        if (meta(7) == ".")
          annot.value(i.toInt)
        else
          "%s;%s" format (meta(7), annot.value(i.toInt))
      v.updateMeta(meta)
      }
    }
  }

  def readAnnot(sites: String)(implicit cnf: Config): Array[String] = {
    val varFile = "%s/%s.variant_function" format (workerDir, sites)
    val exonFile = "%s/%s.exonic_variant_function" format (workerDir, sites)
    val varArr: Array[(String, String)] =
      (for {
        l <- Source.fromFile(varFile).getLines()
        s = l.split("\t")
      } yield (s(0), s(1))).toArray

    val exonMap: Map[Int, String] =
      (for {
        l <- Source.fromFile(exonFile).getLines()
        s = l.split("\t")
      } yield s(0).substring(4).toInt -> s(1))
        .toMap
    varArr.zipWithIndex.map{case ((a, b), i: Int) =>
      if (a == "exonic")
        "ANNO=exonic:%s;GROUP=%s" format (exonMap(i + 1), b)
      else
        "ANNO=%s;GROUP=%s" format (a, b)}
  }
}

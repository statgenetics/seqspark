package org.dizhang.seqa.worker

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.ds.Variant
import org.dizhang.seqa.util.InputOutput._
import org.ini4j.Ini

/**
 * Created by zhangdi on 8/18/15.
 */
object ReadVCF extends Worker[String, RawVCF] {
  implicit val name = new WorkerName("readvcf")

  def apply(input: String)(implicit ini: Ini, sc: SparkContext): RawVCF = {
    val raw = sc.textFile(input)
    makeVariants(raw)
  }

  /** filter variants based on
    * 1. filter column in vcf
    * 2. only bi-allelic SNPs if biAllelicSNV is true in Conf
    */
  def makeVariants(raw: RDD[String])(implicit ini: Ini): RawVCF = {
    val filterNot = ini.get("variant", "filterNot")
    val filter = ini.get("variant", "filter")
    val biAllelicSNV = ini.get("variant", "biAllelicSNV")
    val vars = raw filter (l => ! l.startsWith("#")) map (l => Variant(l))
    val s1 =
      if (filterNot != null)
        vars filter (v => ! v.filter.matches(filterNot))
      else
        vars
    val s2 =
      if (filter != null)
        s1 filter (v => v.filter.matches(filter))
      else
        s1
    val s3 =
      if (biAllelicSNV == "true")
        s2 filter (v => v.ref.matches("[ATCG]") && v.alt.matches("[ATCG]"))
      else
        s2
    /** save is very time-consuming and resource-demanding */
    if (ini.get("general", "save") == "true")
      try {
        s3.saveAsObjectFile("%s/1genotype" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    s3
  }
}

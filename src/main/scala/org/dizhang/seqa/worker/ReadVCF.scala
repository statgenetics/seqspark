package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.ds.{DenseVariant}
import org.dizhang.seqa.util.InputOutput._

/**
 * Always read a dense variant from VCF
 */
object ReadVCF extends Worker[String, RawVCF] {
  implicit val name = new WorkerName("readvcf")

  def apply(input: String)(implicit cnf: Config, sc: SparkContext): RawVCF = {
    val raw = sc.textFile(input)
    makeVariants(raw)
  }

  /** filter variants based on
    * 1. filter column in vcf
    * 2. only bi-allelic SNPs if biAllelicSNV is true in Conf
    */
  def makeVariants(raw: RDD[String])(implicit cnf: Config): RawVCF = {
    val genoInCnf = cnf.getConfig("genotypeInput")
    val filterNot = genoInCnf.getString("filterNot")
    val filter = genoInCnf.getString("filter")
    val biAllelicSNV = genoInCnf.getString("biAllelicSNV")
    val vars = raw filter (l => ! l.startsWith("#")) map (l => DenseVariant(l))
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
    if (cnf.getBoolean("save") == true)
      try {
        s3.saveAsObjectFile(workerDir)
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    s3
  }
}

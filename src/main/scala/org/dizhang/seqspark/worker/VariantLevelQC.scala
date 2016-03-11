package org.dizhang.seqspark.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.ds.{Variant, ByteGenotype, VCF}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.ds.Counter._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.worker.Worker.Data

/**
 * Variant level QC
 */

object VariantLevelQC extends Worker[Data, Data] {

  implicit val name = new WorkerName("variant")

  def apply(data: Data)(implicit cnf: RootConfig, sc: SparkContext): Data = {
    val (geno, pheno) = data
    val input = geno.vars
    val rare = miniQC(input, pheno.batch, _ => true)

    (ByteGenotype(rare, cnf.`import`), pheno)
  }

  def miniQC(vars: RDD[Variant[Byte]],
             batchStr: Array[String],
             mafFunc: Double => Boolean)
            (implicit cnf: RootConfig, sc: SparkContext): RDD[Var] = {

    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val broadCastBatchIdx = sc.broadcast(batchIdx)
    def keyFunc(i: Int): Int = broadCastBatchIdx.value(i)

    val batch = batchIdx
    val misRate = cnf.variantLevelQC.batchMissing

    /** if call rate is high enough */
    def callRateP (v: Var): Boolean = {
      //println("!!! var: %s-%d cnt.length %d" format(v.chr, v.pos, v.cnt.length))
      val cntB = v.toCounter(GenotypeLevelQC.makeCallRate, (1, 1)).reduceByKey(keyFunc)
      val min = cntB.values reduce ((a, b) => if (a._1/a._2 < b._1/b._2) a else b)
      if (min._1/min._2.toDouble < (1 - misRate)) {
        //println("min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        false
      } else {
        //println("!!!min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        true
      }
    }

    /** if maf is high enough and not a batch-specific snv */
    def mafP (v: Var): Boolean = {
      val cnt = v.toCounter(GenotypeLevelQC.makeMaf, (0, 2))
      val cntA = cnt.reduce
      val cntB = cnt.reduceByKey(keyFunc)
      val max = cntB.values reduce ((a, b) => if (a._1 > b._1) a else b)
      val maf = cntA._1/cntA._2.toDouble
      val bSpec =
        if (! v.info.contains("DB") && max._1 > 1 && max._1 == cntA._1) {
          true
        } else {
          //println("bspec var %s-%d %f" format (v.chr, v.pos, max._1))
          false
        }
      //println("!!!!!! var: %s-%d maf: %f" format (v.chr, v.pos, maf))
      if (mafFunc(maf) && ! bSpec) true else false
    }
    //println("there are %d var before miniQC" format (vars.count))
    val snv = vars.filter(v => callRateP(v) && mafP(v))
    //println("there are %d snv passed miniQC" format (snv.count))
    snv
  }
}

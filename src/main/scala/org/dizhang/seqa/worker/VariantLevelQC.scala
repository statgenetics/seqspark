package org.dizhang.seqa.worker

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqa.ds.Count
import org.dizhang.seqa.util.InputOutput._
import org.ini4j.Ini

import scala.io.Source

/**
 * Created by zhangdi on 8/18/15.
 */
object VariantLevelQC extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("variant")

  def apply(input: VCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    val rareMaf = ini.get("variant", "rareMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m < rareMaf || m > (1 - rareMaf)) true else false

    val newPheno = "%s/%s/%s" format(resultsDir, SampleLevelQC.name, ini.get("general", "pheno").split("/").last)
    val afterQC = miniQC(input, ini, newPheno, mafFunc)
    val targetFile = ini.get("variant", "target")
    val select: Boolean = Option(targetFile) match {
      case Some(f) => true
      case None => false
    }

    val rare =
      if (select) {
        val iter = Source.fromFile(targetFile).getLines()
        val res =
          for {l <- iter
               s = l.split("\t")
          } yield "%s-%s".format(s(0), s(1)) -> (s(2) + s(3))
        val vMap = res.toMap
        afterQC.filter(v => vMap.contains("%s-%s".format(v.chr, v.pos)) && vMap("%s-%s".format(v.chr, v.pos)) == (v.ref + v.alt))
      } else
        afterQC

    rare.persist(StorageLevel.MEMORY_AND_DISK_SER)
    /** save is very time-consuming and resource-demanding */
    if (ini.get("variant", "save") == "true")
      try {
        rare.saveAsTextFile("%s/4association" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step3: save failed"); System.exit(1)}
      }
    rare
  }

  def miniQC(vars: VCF, ini: Ini, pheno: String, mafFunc: Double => Boolean): VCF = {
    //val pheno = ini.get("general", "pheno")
    val batchCol = ini.get("pheno", "batch")
    val batchStr = readColumn(pheno, batchCol)
    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val batch = batchIdx
    val misRate = ini.get("variant", "batchMissing").toDouble

    /** if call rate is high enough */
    def callRateP (v: Var): Boolean = {
      //println("!!! var: %s-%d cnt.length %d" format(v.chr, v.pos, v.cnt.length))
      val cntB = Count[Byte, Pair](v, GenotypeLevelQC.makeCallRate).collapseByBatch(batch)
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
      val cnt = Count[Byte, Pair](v, GenotypeLevelQC.makeMaf)
      val cntA = cnt.collapse
      val cntB = cnt.collapseByBatch(batch)
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

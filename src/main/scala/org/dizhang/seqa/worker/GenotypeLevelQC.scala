package org.dizhang.seqa.worker

import java.io.{File, PrintWriter}

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.SparkContext
import org.dizhang.seqa.ds.Count
import org.dizhang.seqa.util.Constant
import Constant.{Bt, Gt}
import org.dizhang.seqa.util.InputOutput._
import org.ini4j.Ini

/**
 * Created by zhangdi on 8/18/15.
 */
object GenotypeLevelQC extends Worker[RawVCF, VCF] {

  implicit val name = new WorkerName("genotype")

  def apply(input: RawVCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    statGdGq(input)
    val gd = ini.get("genotype", "gd").split(",")
    val gq = ini.get("genotype", "gq").toDouble
    val gtFormat = ini.get("genotype", "format").split(":").zipWithIndex.toMap
    val gdLower = gd(0).toDouble
    val gdUpper = gd(1).toDouble
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    def make(g: String): String = {
      val s = g.split(":")
      if (s.length == 1)
        g
      else if (s(gtIdx) == Gt.mis)
        Gt.mis
      else if (s(gdIdx).toInt >= gdLower && s(gdIdx).toInt <= gdUpper && s(gqIdx).toDouble >= gq)
        s(gtIdx)
      else
        Gt.mis
    }

    val res = input.map(v => v.transElem(make(_)).compress(Gt.conv(_)))
    //res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (ini.get("genotype", "save") == "true")
      try {
        res.saveAsObjectFile("%s/2sample" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    res
  }

  /** compute call rate */
  def makeCallRate (g: Byte): Pair = {
    //val gt = g.split(":")(0)
    if (g == Bt.mis)
      (0, 1)
    else
      (1, 1)
  }

  /** compute maf of alt */
  def makeMaf (g: Byte): Pair = {
    //val gt = g.split(":")(0)
    g match {
      case Bt.mis => (0, 0)
      case Bt.ref => (0, 2)
      case Bt.het => (1, 2)
      case Bt.mut => (2, 2)
      case _ => (0, 0)
    }
  }

  /** compute by GD, GQ */
  def statGdGq(vars: RawVCF)(implicit ini: Ini) {
    type Cnt = Int2IntOpenHashMap
    type Bcnt = Map[Int, Cnt]
    val phenoFile = ini.get("general", "pheno")
    val batchCol = ini.get("pheno", "batch")
    val batchStr = readColumn(phenoFile, batchCol)
    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val gtFormat = ini.get("genotype", "format").split(":").zipWithIndex.toMap
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    def make(g: String): Cnt = {
      val s = g split (":")
      if (s(gtIdx) == Gt.mis)
        new Int2IntOpenHashMap(Array(0), Array(1))
      else {
        val key = s(gdIdx).toDouble.toInt * 100 + s(gqIdx).toDouble.toInt
        new Int2IntOpenHashMap(Array(key), Array(1))
      }
    }

    def count(vars: RawVCF): Bcnt = {
      val all = vars map (v => Count[String, Cnt](v, make).collapseByBatch(batchIdx))
      val res = all reduce ((a, b) => Count.addByBatch[Cnt](a, b))
      res
    }

    def writeBcnt(b: Bcnt) {
      val outDir =  workerDir
      val exitCode = "mkdir -p %s".format(outDir).!
      println(exitCode)
      val outFile = "%s/CountByGdGq.txt" format (outDir)
      val pw = new PrintWriter(new File(outFile))
      pw.write("batch\tgd\tgq\tcnt\n")
      for ((i, cnt) <- b) {
        val iter = cnt.keySet.iterator
        while (iter.hasNext) {
          val key = iter.next
          val gd: Int = key / 100
          val gq: Int = key - gd * 100
          val c = cnt.get(key)
          pw.write("%s\t%d\t%d\t%d\n" format (batchKeys(i), gd, gq, c))
        }
      }
      pw.close
    }
    writeBcnt(count(vars))
  }
}

package org.dizhang.seqa.worker

import java.io.{File, PrintWriter}

import com.typesafe.config.Config
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.SparkContext
import org.dizhang.seqa.ds.Counter
import org.dizhang.seqa.util.Constant
import Constant.{Bt, Gt}
import org.dizhang.seqa.util.InputOutput._
import sys.process._

/**
 * Created by zhangdi on 8/18/15.
 */

object GenotypeLevelQC extends Worker[RawVCF, VCF] {

  implicit val name = new WorkerName("genotypeLevelQC")

  def apply(input: RawVCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    statGdGq(input)
    val genoCnf : Config = cnf.getConfig(name.toString)
    val gd = genoCnf.getIntList("gd")
    val gq = genoCnf.getDouble("gq")
    val gtFormat = genoCnf.getString("format").split(":").zipWithIndex.toMap
    val gdLower : Int = gd.get(0)
    val gdUpper : Int = gd.get(1)
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    def make(g: String): Byte = {
      val s = g.split(":")
      if (s.length == 1)
        Gt.conv(g)
      else if (s(gtIdx) == Gt.mis)
        Bt.mis
      else if (s(gdIdx).toInt >= gdLower && s(gdIdx).toInt <= gdUpper && s(gqIdx).toDouble >= gq)
        Gt.conv(s(gtIdx))
      else
        Bt.mis
    }

    val res = input.map(v => v.map(make(_)).toSparseVariant(Bt.ref))
    //res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (genoCnf.getString("save") == "true")
      try {
        res.saveAsObjectFile(workerDir)
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

  def getMaf (p: Pair): Double = {
    if (2 * p._1 <= p._2)
      p._1.toDouble / p._2
    else
      1.0 - (p._1.toDouble / p._2)
  }

  /** compute by GD, GQ */
  def statGdGq(vars: RawVCF)(implicit cnf: Config, sc: SparkContext) {
    type Cnt = Int2IntOpenHashMap
    type Bcnt = Map[Int, Cnt]
    val phenoFile = cnf.getString("sampleInfo.source")
    val batchCol = cnf.getString("sampleInfo.batch")
    val batchStr = readColumn(phenoFile, batchCol)
    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val broadCastBatchIdx = sc.broadcast(batchIdx)
    val gtFormat = cnf.getString("genotypeLevelQC.format").split(":").zipWithIndex.toMap
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

    def keyFunc(i: Int): Int =
      broadCastBatchIdx.value(i)

    def count(vars: RawVCF): Bcnt = {
      val all = vars map (v => v.toCounter(make, Gt.mis).reduceByKey(keyFunc))
      val res = all reduce ((a, b) => Counter.addByKey(a, b))
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

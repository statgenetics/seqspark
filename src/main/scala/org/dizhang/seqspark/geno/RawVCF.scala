package org.dizhang.seqspark.geno

import java.io.{File, PrintWriter}

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Counter, Variant}
import org.dizhang.seqspark.util.Constant.Genotype._
import org.dizhang.seqspark.util.{General, LogicalExpression, SingleStudyContext}
import RawVCF._
import org.dizhang.seqspark.worker._
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 8/15/16.
  */
case class RawVCF(self: RDD[Variant[String]]) extends GeneralizedVCF[String] {


  def decompose(): Data[String] = {
    self.flatMap(v => decomposeVariant(v))
  }

  def checkSex(implicit ssc: SingleStudyContext): Unit = {
    logger.info("check sex ...")
    def isHet(g: String): (Double, Double) = {
      if (g.startsWith(".")) {
        (0,0)
      } else if (g.isHet) {
        (1, 1)
      } else {
        (0, 1)
      }
    }
    Samples.checkSex[String](self, isHet, _.callRate)(ssc)

  }

  def variantsFilter(cond: List[String])(ssc: SingleStudyContext): Data[String] = {
    logger.info("filter variant ...")
    val conf = ssc.userConfig
    val pheno = ssc.phenotype
    val batch = pheno.batch(conf.input.phenotype.batch)
    val controls = pheno.select("control").map{
      case Some("1") => true
      case _ => false
    }
    val myCond = cond.map(c => s"( $c )").reduce((a, b) => s"$a and $b")
    Variants.filter[String](self, myCond, batch, controls, _.maf, _.callRate, _.hwe)
  }

  def statGdGq(ssc: SingleStudyContext): Unit = {
    logger.info("count genotype by DP and GQ ...")

    val sc = ssc.sparkContext
    val conf = ssc.userConfig
    val phenotype = ssc.phenotype
    val batch = conf.input.phenotype.batch
    val (batchKeys, keyFunc) = if (batch == "none") {
      (Array("all"), (i: Int) => 0)
    } else {
      val batchStr = phenotype.batch(batch)
      val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
      val batchMap = batchKeys.zipWithIndex.toMap
      val batchIdx = batchStr.map(b => batchMap(b))
      (batchKeys, (i: Int) => batchIdx(i))
    }
    val all = self.map(v =>
      v.toCounter(makeGdGq(_, v.format), new Int2IntOpenHashMap(Array(0), Array(1)))
        .reduceByKey(keyFunc)(Counter.CounterElementSemiGroup.MapI2I))
    val res = all.reduce((a, b) => Counter.addByKey(a, b)(Counter.CounterElementSemiGroup.MapI2I))
    val outdir = new File(conf.localDir + "/output")
    outdir.mkdir()
    writeBcnt(res, batchKeys, outdir.toString + "/callRate_by_dpgq.txt")
  }

  def genotypeQC(cond: List[String]): RDD[Variant[String]] = {
    if (cond.isEmpty) {
      logger.info("no QC needed for genotype, skip")
      self
    } else {
      logger.info("start genotype qc")
      val all = cond.map{x => s"($x)"}.reduce((a,b) => s"$a and $b")
      self.map { v =>
        v.map { g =>
          val mis = if (g.gt.contains('|')) {
            Raw.diploidPhasedMis
          } else if (g.gt.contains('/')) {
            Raw.diploidUnPhasedMis
          } else {
            Raw.monoploidMis
          }
          g.qc(v.format, all, mis)
        }
      }
      logger.info("done with genotype qc")
    }
  }

  def toSimpleVCF: Data[Byte] = {
    self.map(v => v.map(g => g.toSimpleGenotype))
  }

}

object RawVCF {

  implicit class RawGenotype(val g: String) extends AnyVal {

    def gt: String = g.split(":")(0)

    def toSimpleGenotype: Byte = {
      rawToSimple(g.gt)
    }

    def isMis: Boolean = g.startsWith(".")

    def isDiploid: Boolean = gt.split("[/|]").length == 2

    def isHet: Boolean = (! isMis) && isDiploid && gt.substring(0,1) != gt.substring(2,3)

    def fields(format: String): Map[String, String] = {
      val s = g.split(":")
      val f = format.split(":")
      if (s.length == f.length) {
        f.zip(s).flatMap{
          case (k, v) =>
            val vs = v.split(",")
            if (vs == 2) {
              Array((k + "_1", vs(0)), (k + "_2", vs(1)), (k + "_ratio", (vs(0).toDouble/vs(1).toDouble).toString))
            } else {
              Array((k, v))
            }
        }.toMap
      } else {
        Map(f(0) -> s(0))
      }
    }

    def qc(format: String, cond: String, mis: String): String = {
      val varMap = fields(format)
      if (LogicalExpression.judge(varMap)(cond)) {
        g
      } else {
        mis
      }
    }

    def callRate: (Double, Double) = {
      if (g.startsWith(Raw.monoploidMis))
        (0, 1)
      else
        (1, 1)
    }
    def maf: (Double, Double) = {
      val gt = g.split(":")(0).split("[/|]").map(_.toInt)
      if (gt.length == 1) {
        (gt(0), 1)
      } else {
        (gt.sum, 2)
      }
    }
    def hwe: (Double, Double, Double) = {
      if (isMis) {
        (0, 0, 0)
      } else if (isHet) {
        (0, 1, 0)
      } else if (g.startsWith("0")) {
        (1, 0, 0)
      } else {
        (0, 0, 1)
      }
    }
  }

  def decomposeVariant(v: Variant[String]): Array[Variant[String]] = {
    if (v.alleleNum == 2) {
      Array(v)
    } else {
      val alleles = v.alleles
      (1 until v.alleleNum).toArray.map{i =>
        val newV = v.map{g =>
          val s = g.split(":")
          val gt = s(0).split("[|/]")
          if (gt.length == 1) {
            if (gt(0) == "0") "0" else "1"
          } else {
            gt.map(j => if (j.toInt == i) "1" else "0").mkString(s(0).substring(1,2))
          }
        }
        newV.meta(4) = alleles(i)
        newV
      }
    }
  }

  val gdTicks = (1 to 10).toArray ++ Array(12, 15, 20, 25, 30, 35, 40, 60, 80, 100)
  val gqTicks = Range(0, 100, 5).toArray

  def makeGdGq(g: String, format: String): Int2IntOpenHashMap = {
    val f = g.fields(format)
    if (g.startsWith(".")) {
      new Int2IntOpenHashMap(Array(0), Array(1))
    } else {
      val gd = General.insert(gdTicks, f.getOrElse("DP", "1").toInt)
      val gq = f.getOrElse("GQ", "0").toInt / 5
      val key = 20 * gd + gq
      new Int2IntOpenHashMap(Array(key), Array(1))
    }
  }

  def writeBcnt(b: Map[Int, Int2IntOpenHashMap], batchKeys: Array[String], outFile: String) {
    val pw = new PrintWriter(new File(outFile))
    pw.write("batch\tdp\tgq\tcont\n")
    for ((i, cnt) <- b) {
      val iter = cnt.keySet.iterator
      while (iter.hasNext) {
        val key = iter.next
        val gd: Int = gdTicks(key / 20)
        val gq: Int = gqTicks(key - gd * 20)
        val c = cnt.get(key)
        pw.write("%s\t%d\t%d\t%d\n" format (batchKeys(i), gd, gq, c))
      }
    }
    pw.close()
  }

}

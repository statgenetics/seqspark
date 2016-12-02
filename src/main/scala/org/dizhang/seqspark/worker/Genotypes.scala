package org.dizhang.seqspark.worker

import java.io.{File, PrintWriter}

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Counter, Genotype, Phenotype, Variant}
import org.dizhang.seqspark.util.Constant.Genotype.Raw
import org.dizhang.seqspark.util.{General, LogicalParser, SingleStudyContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Genotype QC functions, currently are only for Raw VCF data
  */
object Genotypes {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  def statGdGq(self: Data[String])(ssc: SingleStudyContext): Unit = {
    logger.info("count genotype by DP and GQ ...")

    val sc = ssc.sparkContext
    val conf = ssc.userConfig
    val phenotype = Phenotype("phenotype")(ssc.sparkSession)
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
    //logger.info("still all right?")
    val first = self.first()
    val fm = first.format.split(":").toList
    val frac: Double = conf.qualityControl.config.getDouble("gdgq.fraction")
    /** this function works on raw data, so limit to a subset can greatly enhance the performance*/
    val all = self.sample(withReplacement = false, frac).map(v =>
      v.toCounter(makeGdGq(_, fm), new Int2IntOpenHashMap(Array(0), Array(1)))
        .reduceByKey(keyFunc))
    //logger.info("going to reduce")
    val res = all.reduce((a, b) => Counter.addByKey(a, b))
    //logger.info("what about now")
    val outdir = new File(conf.localDir + "/output")
    outdir.mkdir()
    writeBcnt(res, batchKeys, outdir.toString + "/callRate_by_dpgq.txt")
  }

  def genotypeQC(self: Data[String], cond: LogicalParser.LogExpr): RDD[Variant[String]] = {
    logger.info("start genotype QC")
    if (cond == LogicalParser.T) {
      logger.info("no need to perform genotype QC")
      self
    } else {
      val first = self.first()
      val fm = first.parseFormat.toList
      val phased = Genotype.Raw.isPhased(first(0))
      val fs = LogicalParser.names(cond)

      self.map { v =>
        v.map { g =>
          val mis = if (phased) {
            Raw.diploidPhasedMis
          } else if (Genotype.Raw.isDiploid(g)) {
            Raw.diploidUnPhasedMis
          } else {
            Raw.monoploidMis
          }
          Genotype.Raw.qc(g, cond, fm, mis)
        }
      }
    }
  }

  def toSimpleVCF(self: Data[String]): Data[Byte] = {
    self.map(v => v.map(g => Genotype.Raw.toSimpleGenotype(g)))
  }

  val gdTicks: Array[Int] = (0 to 10).toArray ++ Array(15, 20, 25, 30, 35, 40, 60, 80, 100)
  val gqTicks: Array[Int] = Range(0, 100, 5).toArray

  def makeGdGq(g: String, format: List[String]): Int2IntOpenHashMap = {
    if (g.startsWith(".")) {
      new Int2IntOpenHashMap(Array(0), Array(1))
    } else {
      val fs = Genotype.Raw.fields(g, format)
      val gd = General.insert(gdTicks, fs.getOrElse("DP", "0").toInt)
      val gq = math.min(fs.getOrElse("GQ", "0").toDouble.toInt, 99) / 5
      val key = 20 * gd + gq
      new Int2IntOpenHashMap(Array(key), Array(1))
    }
  }

  def writeBcnt(b: Map[Int, Int2IntOpenHashMap], batchKeys: Array[String], outFile: String) {
    val pw = new PrintWriter(new File(outFile))
    pw.write("batch\tdp\tgq\tcount\n")
    for ((i, cnt) <- b) {
      val iter = cnt.keySet.iterator
      while (iter.hasNext) {
        val key = iter.next
        val gd: Int = gdTicks(key / 20)
        val gq: Int = gqTicks(key % 20)
        val c = cnt.get(key).toInt
        pw.write("%s\t%d\t%d\t%d\n" format (batchKeys(i), gd, gq, c))
      }
    }
    pw.close()
  }
}

/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.worker

import java.io.PrintWriter

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.Constant.Genotype.Raw
import org.dizhang.seqspark.util.{General, LogicalParser, SeqContext}
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.util.ConfigValue.ImputeMethod
import General._
import org.slf4j.{Logger, LoggerFactory}
import java.nio.file.Path

import org.apache.spark.SparkContext
/**
  * Genotype QC functions, currently are only for Raw VCF data
  */
object Genotypes {

  class GenoCounter[A](f: A => Boolean,
                       private var _value: (Long, Long))
    extends AccumulatorV2[A, (Long, Long)] {

    def add(v: A): Unit = {
      _value = if (f(v)) (value._1 + 1, value._2) else (value._1, value._2 + 1)
    }
    def copy(): GenoCounter[A] = new GenoCounter[A](f, value)

    def isZero: Boolean = value == (0, 0)

    def merge(other: AccumulatorV2[A, (Long, Long)]): Unit = {
      _value = (value._1 + other.value._1, value._2 + other.value._2)
    }

    def reset: Unit = {
      _value = (0, 0)
    }

    def value: (Long, Long) = _value
  }


  val logger: Logger = LoggerFactory.getLogger(getClass)
  def statGdGq(self: Data[String])(ssc: SeqContext): Unit = {
    logger.info("count genotype by DP and GQ ...")

    val sc = ssc.sparkContext
    val conf = ssc.userConfig
    val phenotype = Phenotype("phenotype")(ssc.sparkSession)
    val batch = phenotype.batch(conf.input.phenotype.batch)

    /** broadcast the keyFunc
      *
      * */
    val bcKeyFunc = sc.broadcast(batch.toKeyFunc)


    val frac: Double = conf.qualityControl.config.getDouble("gdgq.fraction")

    /** this function works on raw data, so limit to a subset can greatly enhance the performance */
    val all = self.sample(withReplacement = false, frac).map{v =>
      /** some VCF do have variant specific format */
      val fm = v.format.split(":").toList
      v.toCounter(makeGdGq(_, fm), Map.empty[Int, Long])
        .reduceByKey(bcKeyFunc.value)}
    //logger.info("going to reduce")
    val res = all.reduce((a, b) => Counter.addByKey(a, b))
    //logger.info("what about now")

    writeBcnt(res, batch.keys, conf.output.results.resolve("callRate_by_dpgq.txt"))
  }

  def genotypeQC(self: Data[String],
                 cond: LogicalParser.LogExpr)
                (sc: SparkContext): (RDD[Variant[String]], LongAccumulator) = {
    val varCounter = new LongAccumulator()
    sc.register(varCounter, "Variant counter")

    self.map(_ => varCounter.add(1))

    logger.info("start genotype QC")
    val res =
      if (cond == LogicalParser.T) {
        logger.info("no need to perform genotype QC")
        self
      } else {
        //val first = self.first()
        //val fs = LogicalParser.names(cond)
        logger.info(s"genotype QC criteria: ${LogicalParser.view(cond)}")
        self.map { v =>
          val fm = v.parseFormat.toList
          val phased = Genotype.Raw.isPhased(v(0))
          val rawCnt = v.toCounter(g => if (Genotype.Raw.isMis(g)) 0 else 1, 0).reduce
          val resv = v.map { g =>
            if (g.contains(":")) {
              val mis = if (phased) {
                Raw.diploidPhasedMis
              } else if (Genotype.Raw.isDiploid(g)) {
                Raw.diploidUnPhasedMis
              } else {
                Raw.monoploidMis
              }
              Genotype.Raw.qc(g, cond, fm, mis)
            } else {
              g
            }
          }
          val cnt = resv.toCounter(g => if (Genotype.Raw.isMis(g)) 0 else 1, 0).reduce
          resv.addInfo("SS_CleanGeno", cnt.toString)
          resv.addInfo("SS_RawGeno", rawCnt.toString)
          resv
        }
      }
    (res, varCounter)
  }

  def toSimpleVCF(self: Data[String]): Data[Byte] = {
    self.map(v => v.map(g => Genotype.Raw.toSimpleGenotype(g)))
  }

  def imputeMis(self: Data[Byte])(implicit conf: RootConfig): Data[Byte] = {
    conf.input.genotype.missing match {
      case ImputeMethod.bestGuess =>
        self.map{v =>
          val aaf = v.toCounter(Genotype.Simple.toAAF, (0.0, 2.0)).reduce.ratio
          val target: Byte = if (aaf < 0.5) v.default else (v.default ^ 3).toByte
          v.map(x => if (Genotype.Simple.isMis(x)) target else x)
        }
      case _ => self
    }
  }

  val gdTicks: Array[Int] = (0 to 10).toArray ++ Array(15, 20, 25, 30, 35, 40, 60, 80, 100)
  val gqTicks: Array[Int] = Range(0, 100, 5).toArray

  def makeGdGq(g: String, format: List[String]): Map[Int, Long] = {
    if (g.startsWith(".")) {
      Map.empty[Int, Long]
    } else {
      val fs = Genotype.Raw.fields(g, format)
      val gd = General.insert(gdTicks, fs.getOrElse("DP", "0").toInt)
      val gq = math.min(fs.getOrElse("GQ", "0").toDouble.toInt, 99) / 5
      val key = 20 * gd + gq
      Map(key -> 1L)
    }
  }

  def writeBcnt(b: Map[Byte, Map[Int, Long]], batchKeys: Array[String], outFile: Path) {
    val pw = new PrintWriter(outFile.toFile)
    pw.write("batch\tdp\tgq\tcount\n")
    for ((i, cnt) <- b) {
      val iter = cnt.keySet.iterator
      while (iter.hasNext) {
        val key = iter.next
        val gd: Int = gdTicks(key / 20)
        val gq: Int = gqTicks(key % 20)
        val c = cnt(key)
        pw.write("%s\t%d\t%d\t%d\n" format (batchKeys(i), gd, gq, c))
      }
    }
    pw.close()
  }
}

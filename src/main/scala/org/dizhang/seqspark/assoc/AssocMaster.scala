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

package org.dizhang.seqspark.assoc

import java.io.{File, PrintWriter}

import breeze.linalg.{DenseMatrix, DenseVector, rank, sum}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.assoc.AssocMaster._
import org.dizhang.seqspark.ds.Counter._
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Genotype, Phenotype, SemiGroup}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.util.ConfigValue._
import org.dizhang.seqspark.util.{Constant, LogicalParser, SeqContext}
import org.dizhang.seqspark.worker.Data
import org.slf4j.LoggerFactory

/**
  * asymptotic test
  */
object AssocMaster {
  /** constants */
  val IntPair = SemiGroup.PairInt
  val Permu = Constant.Permutation
  val F = Constant.Annotation.Feature
  val FuncF = Set(F.StopGain, F.StopLoss, F.SpliceSite, F.NonSynonymous)
  val IK = Constant.Variant.InfoKey
  type Imputed = (Double, Double, Double)
  val logger = LoggerFactory.getLogger(getClass)

  def encode[A: Genotype](currentGenotype: Data[A],
                          y: DenseVector[Double],
                          cov: Option[DenseMatrix[Double]],
                          controls: Option[Array[Boolean]],
                          config: MethodConfig)
                         (implicit sc: SparkContext, cnf: RootConfig): RDD[(String, Encode.Coding)] = {

    /** this is quite tricky
      * some encoding methods require phenotype information
      * e.g. to learning weight from data
      * */

    logger.info("start encoding genotype ")
    //val sampleSize = y.value.length
    val codingScheme = config.`type`
    val groupBy = config.misc.groupBy
    val annotated = codingScheme match {
      case MethodType.snv =>
        currentGenotype.map(v => (v.toVariation().toString, Array(v).toIterable))
      case MethodType.meta =>
        currentGenotype.map(v => (s"${v.chr}:${v.pos.toInt/1000000}", v)).groupByKey()
      case _ =>
        (groupBy match {
          case "slidingWindow" :: s => currentGenotype.flatMap(v => v.groupByRegion(s.head.toInt, s(1).toInt))
          case _ => currentGenotype.flatMap(v => v.groupByGene())
        }).groupByKey()
    }

    /**
    val reified = annotated.map(p => p._1 -> p._2.toArray)
    reified.count()
    val msg =
    reified.map{
      case (g, vs) =>
        val no_key = vs.zipWithIndex.filter(p => ! p._1.parseInfo.contains(IK.maf))
          .map(p => (p._2, p._1.pos, p._1.parseInfo.keys.mkString(";")))
        (g, no_key.mkString(","))
    }.filter(_._2.nonEmpty).collect()

    logger.debug(s"${msg.length} like this: ${msg.head.toString()}")
    */
    val bcy = sc.broadcast(y)
    val bcControl = sc.broadcast(controls)
    val bcCov = sc.broadcast(cov)
    val sm: String = config.config.root().render()
    annotated.map(p =>
      (p._1, Encode(p._2, bcControl.value, Option(bcy.value), bcCov.value, sm))
    ).map(x => x._1 -> x._2.getCoding).filter(p => p._2.isDefined && p._2.informative)


  }

  def maxThisLoop(base: Int, i: Int, max: Int, bs: Int): Int = {
    val thisLoopLimit = math.pow(base, i).toInt * bs
    val partialSum = sum(0 to i map(x => math.pow(base, x).toInt)) * bs
    if (partialSum + thisLoopLimit > max)
      max - partialSum
    else
      thisLoopLimit
  }

  class Balancer(val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[Long]
      (k%numPartitions).toInt
    }
  }

  def expand(data: RDD[(String, Encode.Coding)],
             cur: Int, target: Int, jobs: Int): RDD[(String, Encode.Coding)] = {
    /**
      * since the data size is usually small here,
      * we don't worry about shuffling, always re-balance the partitions
      * assume the data is already sorted by the size of each group
      * */
    val sorted = data //.sortBy(p => p._2.size, ascending = false)
    if (target == cur)
      sorted.zipWithIndex().map(_.swap).partitionBy(new Balancer(jobs)).map(_._2)
    else {
      val times = math.ceil(target/cur).toInt
      sorted.flatMap(x =>
        for (i <- 1 to times)
          yield x._1 -> x._2.copy
      ).zipWithIndex().map(_.swap).partitionBy(new Balancer(jobs)).map(_._2)
    }
  }

  def permutationTest(codings: RDD[(String, Encode.Coding)],
                      y: DenseVector[Double],
                      cov: Option[DenseMatrix[Double]],
                      binaryTrait: Boolean,
                      config: MethodConfig)
                     (implicit sc: SparkContext,
                      conf: RootConfig): Map[String, AssocMethod.Result] = {
    logger.info("start permutation test")
    val nm = sc.broadcast(HypoTest.NullModel(y, cov, fit = true, binaryTrait).asInstanceOf[HypoTest.NullModel.Fitted])
    logger.info("get the reference statistics first")
    val jobs = conf.partitions
    val asymptoticRes = asymptoticTest(codings, y, cov, binaryTrait, config).collect().toMap
    val asymptoticStatistic = sc.broadcast(asymptoticRes.map(p => p._1 -> p._2.statistic))
    val intR = """(\d+)""".r
    val sites = conf.association.sites match {
      case intR(n) => n.toInt
      case "exome" => 25000
      case "genome" => 1000000
      case _ => codings.count().toInt
    }
    val max = Permu.max(sites)
    val min = Permu.min(sites)
    val base = Permu.base
    val batchSize = min * base
    val loops = math.round(math.log(max/batchSize)/math.log(base)).toInt
    var curEncode = codings
    var pCount = sc.broadcast(asymptoticRes.map(x => x._1 -> (0, 0)))
    for {
      i <- 0 to loops
      if curEncode.count() > 0
    } {
      val rest = curEncode.count()
      logger.info(s"round $i of permutation test, $rest groups before expand")
      val lastMax = if (i == 0) batchSize else math.pow(base, i - 1).toInt * batchSize
      val curMax = maxThisLoop(base, i, max, batchSize)
      curEncode = expand(curEncode, lastMax, curMax, jobs)
      curEncode.cache()
      logger.info(s"round $i of permutation test, ${curEncode.count()} groups after expand")
      if (conf.debug) {
        val parSpace = curEncode.mapPartitions(p => Array(p.size).toIterator).collect().sorted
        logger.info(s"partition space max: ${parSpace.max} min: ${parSpace.min}, median: ${parSpace(jobs/2)}")
      }
      val copies: Int = math.pow(base, i).toInt
      val curPCount: Map[String, (Int, Int)] = curEncode.map{x =>
        val ref = asymptoticStatistic.value(x._1)
        val minTests = {
          if (i == 0) {
            min
          } else {
            math.max(2 * (min - pCount.value(x._1)._1)/(curMax/batchSize), 1)
          }
        }
        val maxTests = curMax/copies
        val model =
          if (config.`type` == MethodType.snv) {
            SNV(ref, minTests, maxTests, nm.value, x._2)
          } else if (config.maf.getBoolean("fixed")) {
            Burden(ref, minTests, maxTests, nm.value, x._2)
          } else {
            VT(ref, minTests, maxTests, nm.value, x._2)
          }
        x._1 -> model.pCount
      }.reduceByKey((a, b) => IntPair.op(a, b)).collect().toMap

      pCount = sc.broadcast(for ((k, v) <- pCount.value)
        yield if (curPCount.contains(k)) k -> IntPair.op(v, curPCount(k)) else k -> v)
      curEncode = curEncode.filter(x => pCount.value(x._1)._1 < min && pCount.value(x._1)._2 < max)
      //curEncode.persist(StorageLevel.MEMORY_AND_DISK)
    }
    pCount.value.map{x =>
      asymptoticRes(x._1) match {
        case AssocMethod.BurdenAnalytic(v, s, _, _) =>
          (x._1, AssocMethod.BurdenResampling(v, s, x._2))
        case AssocMethod.VTAnalytic(v, n, s, _, _) =>
          (x._1, AssocMethod.VTResampling(v, n, s, x._2))
      }
    }
  }

  def asymptoticTest(codings: RDD[(String, Encode.Coding)],
                     y: DenseVector[Double],
                     cov: Option[DenseMatrix[Double]],
                     binaryTrait: Boolean,
                     config: MethodConfig)
                    (implicit sc: SparkContext,
                     conf: RootConfig): RDD[(String, AssocMethod.Result)] = {
    logger.info("start asymptotic test")
    val fit = config.test == TestMethod.score
    val nm = sc.broadcast(HypoTest.NullModel(y, cov, fit, binaryTrait))

    //logger.info(s"covariates design matrix cols: ${reg.xs.cols}")
    //logger.info(s"covariates design matrix rank: ${rank(reg.xs)}")
    val res: RDD[(String, AssocMethod.Result)] = config.`type` match {
      case MethodType.snv =>
        codings.map(p => (p._1, SNV(nm.value, p._2.asInstanceOf[Encode.Common]).result))
      case MethodType.skat =>
        val method = config.misc.method
        val rho = config.misc.rho
        codings.map(p => (p._1, SKAT(nm.value, p._2.asInstanceOf[Encode.Rare], method, rho).result))
      case MethodType.skato =>
        val method = config.misc.method
        codings.map(p => (p._1, SKATO(nm.value, p._2, method).result))
      case _ =>
        if (config.maf.getBoolean("fixed"))
          codings.map(p => (p._1, Burden(nm.value, p._2).result))
        else
          codings.map(p => (p._1, VT(nm.value, p._2).result))
    }
    res
  }

  def rareMetalWorker(encode: RDD[(String, Encode.Coding)],
                      y: DenseVector[Double],
                      cov: Option[DenseMatrix[Double]],
                      binaryTrait: Boolean,
                      config: MethodConfig)
                     (implicit sc: SparkContext, conf: RootConfig): RDD[SumStat.RMWResult] = {
    val fit = true
    val nm = sc.broadcast(HypoTest.NullModel(y, cov, fit, binaryTrait))
    encode.map(p => SumStat(nm.value, p._2).result)
  }

  def writeResults(res: Seq[(String, AssocMethod.Result)], outFile: File): Unit = {
    //val header = implicitly[AssocMethod.Header[A]]
    if (res.isEmpty) {
      logger.warn(s"no res for file ${outFile.toString}")
    } else {
      val pw = new PrintWriter(outFile)
      pw.write(s"${res.head._2.header}\n")
      res.sortBy(p =>
        p._2.pValue match {
          case None => 2.0
          case Some(x) => x}).foreach{p =>
        pw.write("%s\t%s\n".format(p._1, p._2.toString))
      }
      pw.close()
    }
  }

}

@SerialVersionUID(105L)
class AssocMaster[A: Genotype](genotype: Data[A])(implicit ssc: SeqContext) {
  val cnf = ssc.userConfig
  val sc = ssc.sparkContext
  val phenotype = Phenotype("phenotype")(ssc.sparkSession)

  val logger = LoggerFactory.getLogger(this.getClass)

  val assocConf = cnf.association

  def run(): Unit = {
    val traits = assocConf.traitList
    logger.info("we have traits: %s" format traits.mkString(","))
    traits.foreach{t => runTrait(t)}
  }

  def runTrait(traitName: String): Unit = {
   // try {
    logger.info(s"load trait $traitName from phenotype database")
    phenotype.getTrait(traitName) match {
      case Left(msg) => logger.warn(s"getting trait $traitName failed, skip")
      case Right(tr) =>
        val currentTrait = (traitName, tr)
        val methods = assocConf.methodList
        val indicator = sc.broadcast(phenotype.indicate(traitName))
        val controls = if (phenotype.contains(Pheno.control)) {
          Some(phenotype.select(Pheno.control).zip(indicator.value)
            .filter(p => p._2).map(p => if (p._1.get == "1") true else false))
        } else {
          None
        }

        val chooseSample = genotype.samples(indicator.value)(sc)
        val cond = LogicalParser.parse("informative")
        val currentGenotype = chooseSample.variants(cond, None, true)(ssc)
        currentGenotype.persist(StorageLevel.MEMORY_AND_DISK)
        val traitConfig = assocConf.`trait`(traitName)
        val cov =
          if (traitConfig.covariates.nonEmpty || traitConfig.pc > 0) {
            val all = traitConfig.covariates ++ (1 to traitConfig.pc).map(i => s"_pc$i")
            phenotype.getCov(traitName, all, Array.fill(all.length)(0.0)) match {
              case Left(msg) =>
                logger.warn(s"failed getting covariates for $traitName, nor does PC specified. use None")
                None
              case Right(dm) =>
                logger.debug(s"COV dimension: ${dm.rows} x ${dm.cols}")
                Some(dm)
            }
          } else
            None
        val currentCov = cov
        methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m)(sc, cnf))
    }
  }

  def runTraitWithMethod(currentGenotype: Data[A],
                         currentTrait: (String, DenseVector[Double]),
                         cov: Option[DenseMatrix[Double]],
                         controls: Option[Array[Boolean]],
                         method: String)(implicit sc: SparkContext, cnf: RootConfig): Unit = {
    logger.info(s"prepare data for association tests for ${currentTrait._1} with method $method")
    val config = cnf.association
    val methodConfig = config.method(method)
    val cond = LogicalParser.parse(methodConfig.misc.variants)
    val chosenVars = currentGenotype.variants(cond, None, true)(ssc)

    if (cnf.benchmark) {
      logger.debug(s"${chosenVars.count()} variants were selected for testing assocition for ${currentTrait._1} with $method")
    }

    val codings = encode(chosenVars, currentTrait._2, cov, controls, methodConfig)

    val outDir = cnf.output.results

    codings.cache()


    val permutation = methodConfig.resampling
    val clean = if (permutation) {
      codings.sortBy(p => p._2.size, ascending = false)
        .zipWithIndex().map(_.swap).partitionBy(new Balancer(cnf.partitions)).map(_._2)
    } else {
      codings
    }

    clean.cache()

    if (cnf.benchmark) {
      logger.info(s"encoding completed ${clean.count()} groups")
    }
    if (cnf.debug) {
      val path = cnf.input.genotype.path + s".${currentTrait._1}.$method"
      try {
        codings.map{
          case (g, c) =>
            s"$g," + c.toString
        }.saveAsTextFile(path)
      } catch {
        case e: Exception => logger.warn(s"$path exists")
      }
    }

      val summary = codings.map{
        case (g, Encode.VT(c, v)) =>
          s"$g variants: ${v.length} thresholds: ${c.length}"
        case (g, e) =>
          s"$g variants: ${e.numVars}"
      }.collect()

      val pw = new PrintWriter(outDir.resolve(s"encode_${method}_summary.txt").toFile)
      for (s <- summary) {
        pw.write(s"$s\n")
      }
      pw.close()

    val test = methodConfig.test
    val traitName = currentTrait._1
    val binary = config.`trait`(traitName).binary
    logger.info(s"run trait ${traitName} with method $method")

    if (methodConfig.`type` == MethodType.meta) {
      val res = rareMetalWorker(clean, currentTrait._2, cov, binary, methodConfig)(sc, cnf)
      res.cache()
      res.saveAsObjectFile(cnf.input.genotype.path + s".${cnf.project}.summary.$traitName")
      res.saveAsTextFile(cnf.input.genotype.path + s".${cnf.project}.summary.$traitName.txt")
    } else if (permutation) {
      val res = permutationTest(clean, currentTrait._2, cov, binary, methodConfig)(sc, cnf).toArray
      writeResults(res.map(p => p._1 -> p._2), outDir.resolve(s"assoc_${currentTrait._1}_${method}_perm").toFile)
    } else {
      val res = asymptoticTest(clean, currentTrait._2, cov, binary, methodConfig)(sc, cnf).collect()
      writeResults(res.map(p => p._1 -> p._2), outDir.resolve(s"assoc_${currentTrait._1}_${method}").toFile)
    }
    logger.info(s"finished association tests for ${currentTrait._1} using $method")
  }
}

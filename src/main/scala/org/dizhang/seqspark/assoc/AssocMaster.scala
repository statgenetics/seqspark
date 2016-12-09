package org.dizhang.seqspark.assoc

import java.io.{File, PrintWriter}

import breeze.linalg.{DenseMatrix, DenseVector, rank, sum}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.assoc.AssocMaster._
import org.dizhang.seqspark.ds.Counter._
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Genotype, Phenotype}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.util.{Constant, LogicalParser, SingleStudyContext}
import org.dizhang.seqspark.worker.Data
import org.slf4j.LoggerFactory

/**
  * asymptotic test
  */
object AssocMaster {
  /** constants */
  val IntPair = CounterElementSemiGroup.PairInt
  val Permu = Constant.Permutation
  val F = Constant.Annotation.Feature
  val FuncF = Set(F.StopGain, F.StopLoss, F.SpliceSite, F.NonSynonymous)
  val IK = Constant.Variant.InfoKey
  type Imputed = (Double, Double, Double)
  val logger = LoggerFactory.getLogger(getClass)

  def encode[A: Genotype](currentGenotype: Data[A],
                          y: Broadcast[DenseVector[Double]],
                          cov: Broadcast[Option[DenseMatrix[Double]]],
                          controls: Broadcast[Array[Boolean]],
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

    val sm: String = config.config.root().render()
    annotated.map(p =>
      (p._1, Encode(p._2, Option(controls.value), Option(y.value), cov.value, sm))
    ).map(x => x._1 -> x._2.getCoding).filter(p => p._2.isDefined && p._2.informative)


  }

  def adjustForCov(binaryTrait: Boolean,
                   y: DenseVector[Double],
                  cov: DenseMatrix[Double]): Regression = {
      if (binaryTrait) {
        LogisticRegression(y, cov)
      } else {
        LinearRegression(y, cov)
      }
  }

  def maxThisLoop(base: Int, i: Int, max: Int, bs: Int): Int = {
    val thisLoopLimit = math.pow(base, i).toInt * bs
    val partialSum = sum(0 to i map(x => math.pow(base, x).toInt)) * bs
    if (partialSum + thisLoopLimit > max)
      max - partialSum
    else
      thisLoopLimit
  }

  class Balancer(val numPartitions: Int, val rest: Long) extends Partitioner {
    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[Long]
      val space = if (rest%numPartitions == 0) rest/numPartitions else rest/numPartitions + 1
      (k/space).toInt
    }
  }

  def expand(data: RDD[(String, Encode.Coding)],
             cur: Int, target: Int, rest: Long, jobs: Int): RDD[(String, Encode.Coding)] = {
    /**
      * since the data size is usually small here,
      * we don't worry about shuffling, always re-balance the partitions
      * */
    val sorted = data.sortBy(p => p._2.size, ascending = false)
    if (target == cur)
      sorted.zipWithIndex().map(_.swap).partitionBy(new Balancer(jobs, rest)).map(_._2)
    else {
      val times = math.ceil(target/cur).toInt
      sorted.flatMap(x =>
        for (i <- 1 to times)
          yield x._1 -> x._2.copy
      ).zipWithIndex().map(_.swap).partitionBy(new Balancer(jobs, rest * times)).map(_._2)
    }
  }

  def permutationTest(codings: RDD[(String, Encode.Coding)],
                      y: Broadcast[DenseVector[Double]],
                      cov: Broadcast[Option[DenseMatrix[Double]]],
                      binaryTrait: Boolean,
                      controls: Broadcast[Array[Boolean]],
                      config: MethodConfig)
                     (implicit sc: SparkContext, conf: RootConfig): Map[String, AssocMethod.ResamplingResult] = {
    logger.info("start permutation test")
    val reg = adjustForCov(binaryTrait, y.value, cov.value.get)
    val nm = ScoreTest.NullModel(reg)
    logger.info("get the reference statistics first")
    val jobs = conf.jobs
    val asymptoticRes = asymptoticTest(codings, y, cov, binaryTrait, config).collect().toMap
    val asymptoticStatistic = sc.broadcast(asymptoticRes.map(p => p._1 -> p._2.statistic))
    val intR = """(\d+)""".r
    val sites = conf.association.sites match {
      case intR(n) => n.toInt
      case "exome" => 25000
      case "genome" => 1000000
      case _ => codings.count().toInt
    }
    val max = Permu.max2(sites)
    val min = Permu.min2(sites)
    val base = Permu.base
    val batchSize = min * base
    val loops = math.round(math.log(sites)/math.log(base)).toInt
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
      curEncode = expand(curEncode, lastMax, curMax, rest, jobs)
      curEncode.cache()
      logger.info(s"round $i of permutation test, ${curEncode.count()} groups after expand")
      if (conf.debug) {
        val parSpace = curEncode.mapPartitions(p => Array(p.size).toIterator).collect().sorted
        logger.info(s"partition space max: ${parSpace.max} min: ${parSpace.min}, median: ${parSpace(jobs/2)}")
      }
      val curPCount: Map[String, (Int, Int)] = curEncode.map{x =>
        val ref = asymptoticStatistic.value(x._1)
        val minTests = {
          if (i == 0) {
            min
          } else {
            math.max(2 * (min - pCount.value(x._1)._1)/(curMax/batchSize), 1)
          }
        }
        val model =
          if (config.`type` == MethodType.snv) {
            SNV(ref, minTests, batchSize, nm, x._2)
          } else if (config.maf.getBoolean("fixed")) {
            Burden(ref, minTests, batchSize, nm, x._2)
          } else {
            VT(ref, minTests, batchSize, nm, x._2)
          }
        x._1 -> model.pCount
      }.reduceByKey((a, b) => IntPair.op(a, b)).collect().toMap

      pCount = sc.broadcast(for ((k, v) <- pCount.value)
        yield if (curPCount.contains(k)) k -> IntPair.op(v, curPCount(k)) else k -> v)
      curEncode = curEncode.filter(x => pCount.value(x._1)._1 < min)
      //curEncode.persist(StorageLevel.MEMORY_AND_DISK)
    }
    pCount.value.map{x =>
      val asymp = asymptoticRes(x._1)
      val vars = asymp.vars
      val ref = asymp.statistic
      (x._1, AssocMethod.ResamplingResult(vars, ref, x._2))
    }
  }

  def asymptoticTest(codings: RDD[(String, Encode.Coding)],
                     y: Broadcast[DenseVector[Double]],
                     cov: Broadcast[Option[DenseMatrix[Double]]],
                     binaryTrait: Boolean,
                     config: MethodConfig)
                    (implicit sc: SparkContext, conf: RootConfig): RDD[(String, AssocMethod.AnalyticResult)] = {
    logger.info("start asymptotic test")
    val reg = adjustForCov(binaryTrait, y.value, cov.value.get)
    //logger.info(s"covariates design matrix cols: ${reg.xs.cols}")
    //logger.info(s"covariates design matrix rank: ${rank(reg.xs)}")
    config.`type` match {
      case MethodType.snv =>
        val nm = sc.broadcast(ScoreTest.NullModel(reg))
        codings.map(p => (p._1, SNV.AnalyticTest(nm.value, p._2.asInstanceOf[Encode.Common]).result))
      case MethodType.skat =>
        val nm = sc.broadcast(ScoreTest.NullModel(reg))
        val method = config.misc.method
        codings.map(p => (p._1, SKAT(nm.value, p._2, method, 0.0).result))
      case MethodType.skato =>
        val snm = sc.broadcast(SKATO.NullModel(reg))
        val method = config.misc.method
        codings.map(p => (p._1, SKATO(snm.value, p._2, method).result))
      case _ =>
        val nm = sc.broadcast(ScoreTest.NullModel(reg))
        if (config.maf.getBoolean("fixed"))
          codings.map(p => (p._1, Burden(nm.value, p._2).result))
        else
          codings.map(p => (p._1, VT(nm.value, p._2).result))
    }
  }
/**
  def rareMetalWorker(encode: RDD[(String, Encode.Coding)],
                      y: Broadcast[DenseVector[Double]],
                      cov: Broadcast[Option[DenseMatrix[Double]]],
                      binaryTrait: Boolean,
                      config: MethodConfig)(implicit sc: SparkContext): Unit = {
    val reg = adjustForCov(binaryTrait, y.value, cov.value.get)
    val nm = sc.broadcast(ScoreTest.NullModel(reg))
    encode.map(p => (p._1, RareMetalWorker.Analytic(nm.value, p._2).result))
  }
*/
  def writeResults(res: Seq[(String, AssocMethod.Result)], outFile: String): Unit = {
    val pw = new PrintWriter(new java.io.File(outFile))
    res match {
      case _: AssocMethod.AnalyticResult => pw.write("name\tvars\tstatistic\tp-value\n")
      case _: AssocMethod.ResamplingResult => pw.write("name\tvars\tstatistic\tp-count\tp-value\n")
      case _ => pw.write("name\tvars\tstatistic\tp-value\n")
    }
    res.sortBy(p => p._2.pValue match {case None => 2.0; case Some(x) => x}).foreach{p =>
      pw.write("%s\t%s\n".format(p._1, p._2.toString))
    }
    pw.close()
  }

}

@SerialVersionUID(105L)
class AssocMaster[A: Genotype](genotype: Data[A])(ssc: SingleStudyContext) {
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
        val currentTrait = (traitName, sc.broadcast(tr))
        val methods = assocConf.methodList
        val indicator = sc.broadcast(phenotype.indicate(traitName))
        val controls = sc.broadcast(phenotype.select(Pheno.Header.control).zip(indicator.value)
          .filter(p => p._2).map(p => if (p._1.get == "1") true else false))
        val chooseSample = genotype.samples(indicator.value)(sc)
        val cond = LogicalParser.parse("informative")
        val currentGenotype = chooseSample.variants(cond)(ssc)
        currentGenotype.persist(StorageLevel.MEMORY_AND_DISK)
        val traitConfig = assocConf.`trait`(traitName)
        /**
        val pc = if (traitConfig.pc == 0) {
          None
        } else {
          logger.info("perform PCA for genotype data")
          val forPCA = currentGenotype
            .variants(List("(maf >= 0.01 or maf <= 0.99) and chr != \"X\" and chr != \"Y\""))(ssc)
          val res =  new PCA(forPCA).pc(traitConfig.pc)
          logger.info(s"PC dimension: ${res.rows} x ${res.cols}")
          Some(res)
        }
          */
        val cov =
          if (traitConfig.covariates.nonEmpty || traitConfig.pc > 0) {
            val all = traitConfig.covariates ++ (1 to traitConfig.pc).map(i => s"_pc$i")
            phenotype.getCov(traitName, all, Array.fill(all.length)(0.05)) match {
              case Left(msg) =>
                logger.warn(s"failed getting covariates for $traitName, nor does PC specified. use None")
                None
              case Right(dm) =>
                logger.info(s"COV dimension: ${dm.rows} x ${dm.cols}")
                Some(dm)
            }
          } else
            None
        val currentCov = sc.broadcast(cov)
        methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m)(sc, cnf))
    }
  }

  def runTraitWithMethod(currentGenotype: Data[A],
                         currentTrait: (String, Broadcast[DenseVector[Double]]),
                         cov: Broadcast[Option[DenseMatrix[Double]]],
                         controls: Broadcast[Array[Boolean]],
                         method: String)(implicit sc: SparkContext, cnf: RootConfig): Unit = {
    val config = cnf.association
    val methodConfig = config.method(method)
    val cond = LogicalParser.parse(methodConfig.misc.variants)
    val chosenVars = currentGenotype.variants(cond)(ssc)
    val codings = encode(chosenVars, currentTrait._2, cov, controls, methodConfig)


    codings.cache()
    val cnt = codings.count()
    if (cnf.benchmark) {
      logger.info(s"encoding completed $cnt groups")
    }

    val sorted = codings.sortBy(p => p._2.size, ascending = false)
      .zipWithIndex().map(_.swap).partitionBy(new Balancer(cnf.jobs, cnt)).map(_._2)

    if (method == "vt") {
      val summary = codings.map{case (g, e) =>
        val vt = e.asInstanceOf[Encode.VT]
        s"$g variants: ${vt.vars.length} thresholds: ${vt.coding.length}"}.collect()
      val pw = new PrintWriter(new File(s"output/encode_${method}_summary.txt"))
      for (s <- summary) {
        pw.write(s"$s\n")
      }
      pw.close()
    }

    val permutation = methodConfig.resampling
    val test = methodConfig.test
    val binary = config.`trait`(currentTrait._1).binary
    logger.info(s"run trait ${currentTrait._1} with method $method")
    if (permutation) {
      val res = permutationTest(sorted, currentTrait._2, cov, binary, controls, methodConfig)(sc, cnf).toArray
      writeResults(res, s"output/assoc_${currentTrait._1}_${method}_perm")
    } else {
      val res = asymptoticTest(sorted, currentTrait._2, cov, binary, methodConfig)(sc, cnf).collect()
      writeResults(res, s"output/assoc_${currentTrait._1}_$method")
    }
  }
}

package org.dizhang.seqspark.assoc

import breeze.linalg.{sum, DenseMatrix, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{VCF, Phenotype}
import org.dizhang.seqspark.stat._
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.worker.Annotation
import AssocTest._
import org.dizhang.seqspark.ds.Counter._

/**
  * asymptotic test
  */
object AssocTest {
  /** constants */
  val IntPair = CounterElementSemiGroup.PairInt
  val Permu = Constant.Permutation
  val F = Constant.Annotation.Feature
  val FuncF = Set(F.StopGain, F.StopLoss, F.SpliceSite, F.NonSynonymous)
  val IK = Constant.Variant.InfoKey

  def makeEncode(currentGenotype: VCF,
                 y: Broadcast[DenseVector[Double]],
                 cov: Broadcast[Option[DenseMatrix[Double]]],
                 controls: Broadcast[Array[Boolean]],
                 config: MethodConfig)(implicit sc: SparkContext, cnf: RootConfig): RDD[(String, Encode)] = {

    /** this is quite tricky
      * some encoding methods require phenotype information
      * e.g. to learning weight from data */
    val sampleSize = y.value.length
    val codingScheme = config.coding
    val annotated = codingScheme match {
      case CodingMethod.single =>
        currentGenotype.vars.map(v => (s"${v.chr}-${v.pos}", v)).groupByKey()
      case _ => Annotation.forAssoc(currentGenotype).vars
          .map(v => (v.parseInfo(IK.gene), v))
          .filter(x => FuncF.contains(F.withName(x._2.parseInfo(IK.func))))
          .groupByKey()
    }

    annotated.map(p =>
      (p._1, Encode(p._2, sampleSize, Option(controls.value), Option(y.value), cov.value, config))
    )
  }

  def adjustForCov(binaryTrait: Boolean,
                  y: DenseVector[Double],
                  cov: Option[DenseMatrix[Double]]): Option[DenseVector[Double]] = {
    cov match {
      case None => None
      case Some(c) => Some(
        if (binaryTrait)
          new LogisticRegression(y, c).estimates
        else
          new LinearRegression(y, c).estimates)
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

  def expand(data: RDD[(String, Encode)], cur: Int, target: Int): RDD[(String, Encode)] = {
    if (target == cur)
      data
    else
      data.map(x => Array.fill(math.ceil(target/cur).toInt)(x)).flatMap(x => x)
  }


  def permutationTest(encode: RDD[(String, Encode)],
                      y: Broadcast[DenseVector[Double]],
                      cov: Broadcast[Option[DenseMatrix[Double]]],
                      binaryTrait: Boolean,
                      controls: Broadcast[Array[Boolean]],
                      config: MethodConfig)(implicit sc: SparkContext): Map[String, TestResult] = {
    val estimates = adjustForCov(binaryTrait, y.value, cov.value)
    val asymptoticRes = asymptoticTest(encode, y, cov, binaryTrait, TestMethod.score)
    val asymptoticStatistic = sc.broadcast(asymptoticRes.map(x => x._1 -> x._2.statistic))
    val sites = encode.count().toInt
    val max = Permu.max(sites)
    val min = Permu.min(sites)
    val base = Permu.base
    val batchSize = min * base
    val loops = math.round(math.log(sites)/math.log(base)).toInt
    var curEncode = encode
    var pCount = sc.broadcast(asymptoticRes.map(x => x._1 -> (0, 0)))
    for (i <- 0 to loops) {
      val lastMax = if (i == 0) batchSize else math.pow(base, i - 1).toInt * batchSize
      val curMax = maxThisLoop(base, i, max, batchSize)
      curEncode = expand(curEncode, lastMax, curMax)
      val curPCount = curEncode
        .map{x =>
          val ref = asymptoticStatistic.value(x._1)
          val model = Resampling(ref,
            min - pCount.value(x._1)._1,
            batchSize,
            x._2, y.value,
            cov.value,
            estimates,
            Option(controls.value),
            Option(config),
            binaryTrait)
          (x._1, model.pCount)}
        .reduceByKey((a, b) => IntPair.op(a, b)).collect().toMap
      pCount = sc.broadcast(for ((k, v) <- pCount.value)
        yield if (curPCount.contains(k)) k -> IntPair.op(v, curPCount(k)) else k -> v)
      curEncode = curEncode.filter(x => pCount.value(x._1)._1 < min)
    }
    pCount.value.map(x => (x._1, TestResult(None, None, x._2._2.toDouble, x._2._1.toDouble/x._2._2)))
  }

  def asymptoticTest(encode: RDD[(String, Encode)],
                     y: Broadcast[DenseVector[Double]],
                     cov: Broadcast[Option[DenseMatrix[Double]]],
                     binaryTrait: Boolean,
                     test: TestMethod.Value): Map[String, TestResult] = {

    val coding = encode.map(p => (p._1, p._2.getCoding.get))
    val estimates = adjustForCov(binaryTrait, y.value, cov.value)
    test match {
      case TestMethod.score =>
        coding.map(p => (p._1, ScoreTest(binaryTrait, y.value, p._2, cov.value, estimates).summary))
          .collect().toMap
    }
  }

  def runTraitWithMethod(currentGenotype: VCF,
                         currentTrait: (String, Broadcast[DenseVector[Double]]),
                         cov: Broadcast[Option[DenseMatrix[Double]]],
                         controls: Broadcast[Array[Boolean]],
                         method: String)(implicit sc: SparkContext, cnf: RootConfig): Unit = {
    val config = cnf.association
    val methodConfig = config.method(method)
    val encode = makeEncode(currentGenotype, currentTrait._2, cov, controls, methodConfig)
    val permutation = methodConfig.resampling
    val test = methodConfig.test
    val binary = config.`trait`(currentTrait._1).binary

    if (permutation) {
      permutationTest(encode, currentTrait._2, cov, binary, controls, methodConfig)
    } else {
      asymptoticTest(encode, currentTrait._2, cov, binary, test)
    }
  }
}

class AssocTest(genotype: VCF,
                phenotype: Phenotype)
               (implicit cnf: RootConfig,
                sc: SparkContext) extends Assoc {

  val assocConf = cnf.association

  def run(): Unit = {
    val traits = assocConf.traitList
    traits.foreach{
      t => runTrait(t)
    }
  }

  def runTrait(traitName: String) = {
    try {
      logger.info(s"load trait $traitName from phenotype database")
      val currentTrait = (traitName, sc.broadcast(phenotype.makeTrait(traitName)))
      val methods = assocConf.methodList
      val indicator = sc.broadcast(phenotype.indicate(traitName))
      val controls = sc.broadcast(phenotype(Pheno.Header.control).zip(indicator.value)
        .filter(p => p._2).map(p => if (p._1.get == 1.0) true else false))
      val currentGenotype = genotype.select(indicator.value)
      val traitConfig = assocConf.`trait`(traitName)
      val currentCov = sc.broadcast(
        if (traitConfig.covariates.nonEmpty)
          Some(phenotype.makeCov(traitName, traitConfig.covariates))
        else
          None)
      methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m))
    } catch {
      case e: Exception => logger.warn(e.getStackTrace.toString)
    }
  }
}

package org.dizhang.seqa.assoc

import breeze.linalg.{sum, DenseMatrix, DenseVector}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.assoc.Encode.{Fixed, VT}
import org.dizhang.seqa.stat._
import org.dizhang.seqa.util.Constant
import org.dizhang.seqa.util.Constant.{UnPhased, Pheno}
import org.dizhang.seqa.util.InputOutput.VCF
import org.dizhang.seqa.worker.GenotypeLevelQC.{getMaf, makeMaf}
import collection.JavaConverters._
import AssocTest._
import org.dizhang.seqa.ds.Counter._

/**
  * asymptotic test
  */
object AssocTest {
  /** constants */
  val CPSomeTrait = Constant.ConfigPath.Association.SomeTrait
  val CPTrait = Constant.ConfigPath.Association.`trait`
  val CPTraitList = Constant.ConfigPath.Association.Trait.list
  val CPSomeMethod = Constant.ConfigPath.Association.SomeMethod
  val CPMethod = Constant.ConfigPath.Association.method
  val CPMethodList = Constant.ConfigPath.Association.Method.list
  val CVSomeTrait = Constant.ConfigValue.Association.SomeTrait
  val CVSomeMethod = Constant.ConfigValue.Association.SomeMethod
  val IntPair = CounterElementSemiGroup.PairInt
  val Permu = Constant.Permutation



  def makeEncode(currentGenotype: VCF,
             y: Broadcast[DenseVector[Double]],
             cov: Broadcast[Option[DenseMatrix[Double]]],
             controls: Broadcast[Array[Boolean]],
             methodConfig: Config)(implicit sc: SparkContext): RDD[(String, Encode)] = {

    /** this is quite tricky
      * some encoding methods require phenotype information
      * e.g. to learning weight from data */
    val sampleSize = y.value.length
    val codingScheme = methodConfig.getString(CPSomeMethod.coding)
    val annotated = codingScheme match {
      case CVSomeMethod.Coding.single => currentGenotype.map(v => (v.pos, v)).groupByKey()
      case _ => currentGenotype.map(v => (v.parseInfo(Constant.Variant.InfoKey.gene), v)).groupByKey()}

    annotated.map(p => (p._1, Encode(p._2, sampleSize, Option(controls.value), Option(y.value), cov.value, methodConfig)))
      .filter(p => p._2.isDefined)
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
                      methodConfig: Config)(implicit sc: SparkContext): Map[String, (Int, Int)] = {
    val estimates = adjustForCov(binaryTrait, y.value, cov.value)
    val asymptoticRes = asymptoticTest(encode, y, cov, binaryTrait, CVSomeMethod.Test.score).collect()
    val asymptoticStatistic = sc.broadcast(asymptoticRes.map(x => x._1 -> x._2.statistic).toMap)
    val sites = encode.count().toInt
    val max = Permu.max(sites)
    val min = Permu.min(sites)
    val base = Permu.base
    val batchSize = min * base
    val loops = math.round(math.log(sites)/math.log(base)).toInt
    var curEncode = encode
    var pCount = sc.broadcast(asymptoticRes.map(x => x._1 -> (0, 0)).toMap)
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
            Option(methodConfig),
            binaryTrait)
          (x._1, model.pCount)}
        .reduceByKey((a, b) => IntPair.op(a, b)).collect().toMap
      pCount = sc.broadcast(for ((k, v) <- pCount.value)
        yield if (curPCount.contains(k)) k -> IntPair.op(v, curPCount(k)) else k -> v)
      curEncode = curEncode.filter(x => pCount.value(x._1)._1 < min)
    }
    pCount.value
  }

  def asymptoticTest(encode: RDD[(String, Encode)],
                     y: Broadcast[DenseVector[Double]],
                     cov: Broadcast[Option[DenseMatrix[Double]]],
                     binaryTrait: Boolean,
                     test: String): RDD[(String, TestResult)] = {

    val coding = encode.map(p => (p._1, p._2.getCoding.get))
    val estimates = adjustForCov(binaryTrait, y.value, cov.value)
    test match {
      case CVSomeMethod.Test.score =>
        coding.map(p => (p._1, ScoreTest(binaryTrait, y.value, p._2, cov.value, estimates).summary))
    }
  }

  def runTraitWithMethod(currentGenotype: VCF,
                         currentTrait: (String, Broadcast[DenseVector[Double]]),
                         cov: Broadcast[Option[DenseMatrix[Double]]],
                         controls: Broadcast[Array[Boolean]],
                         method: String)(implicit sc: SparkContext, config: Config): Unit = {
    val methodConfig = config.getConfig(s"$CPMethod.$method")
    val encode = makeEncode(currentGenotype, currentTrait._2, cov, controls, methodConfig)
    val permutation = methodConfig.getBoolean(CPSomeMethod.permutation)
    val test = methodConfig.getString(CPSomeMethod.test)
    val binary = config.getBoolean(s"$CPTrait.${currentTrait._1}.${CPSomeTrait.binary}")

    if (permutation) {
      permutationTest(encode, currentTrait._2, cov, binary, controls, methodConfig)
    } else {
      asymptoticTest(encode, currentTrait._2, cov, binary, test)
    }
  }
}

class AssocTest(genotype: VCF, phenotype: Phenotype)(implicit config: Config, sc: SparkContext) extends Assoc {
  def run: Unit = {
    val traits = config.getStringList(CPTraitList).asScala.toArray
    traits.foreach{
      t => runTrait(t)
    }
  }

  def runTrait(traitName: String) = {
    try {
      logger.info(s"load trait $traitName from phenotype database")
      val currentTrait = (traitName, sc.broadcast(phenotype.makeTrait(traitName)))
      val methods = config.getStringList(CPMethodList).asScala.toArray
      val indicator = sc.broadcast(phenotype.indicate(traitName))
      val controls = sc.broadcast(phenotype(Pheno.Header.control).zip(indicator.value)
        .filter(p => p._2).map(p => if (p._1.get == 1.0) true else false))
      val currentGenotype = genotype.map(v => v.select(indicator.value))
      val ConfigPathCov = s"$CPTrait.$traitName.${CPSomeTrait.covariates}"
      val currentCov = sc.broadcast(
        if (config.hasPath(ConfigPathCov))
          Some(phenotype.makeCov(traitName, config.getStringList(ConfigPathCov).asScala.toArray))
        else
          None)
      methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m))
    } catch {
      case e: Exception => logger.warn(e.getStackTrace.toString)
    }
  }
}

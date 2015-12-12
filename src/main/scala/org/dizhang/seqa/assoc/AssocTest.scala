package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
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

  def computePCount()

  def permutationTest(encode: RDD[(String, Encode)],
                      y: Broadcast[DenseVector[Double]],
                      cov: Broadcast[Option[DenseMatrix[Double]]],
                      binaryTrait: Boolean,
                      controls: Broadcast[Array[Boolean]],
                      methodConfig: Config): RDD[(String, TestResult)] = {
    val estimates = adjustForCov(binaryTrait, y.value, cov.value)
    val asymptoticRes = asymptoticTest(encode, y, cov, binaryTrait, CVSomeMethod.Test.score).collect()
    val asymptoticStatistic = asymptoticRes.map(x => x._2.statistic)
    val sites = encode.count().toInt
    val max = Permu.max(sites)
    val min = Permu.min(sites)
    val alpha = Permu.alpha
    val base = Permu.base
    val batchSize = base * min
    val loops = math.round(math.log(sites)/math.log(base)).toInt
    for (i <- 0 to loops) {
      var maxThisLoop = math.pow(base, i).toInt

    }
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
                         method: String)(implicit sc: SparkContext, config: Config): RDD[(String, TestResult)] = {
    val methodConfig = config.getConfig(s"$CPMethod.$method")
    val encode = makeEncode(currentGenotype, currentTrait._2, cov, controls, methodConfig)
    val permutation = methodConfig.getBoolean(CPSomeMethod.permutation)
    val test = methodConfig.getString(CPSomeMethod.test)
    val binary = config.getBoolean(s"$CPTrait.${currentTrait._1}.${CPSomeTrait.binary}")

    if (permutation) {
      asymptoticTest(encode, currentTrait._2, cov, binary, test)
    } else {
      asymptoticTest(encode, currentTrait._2, cov, binary, test)
    }
  }
}

class AssocTest(genotype: VCF, phenotype: Phenotype)(implicit config: Config, sc: SparkContext) extends Assoc {
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

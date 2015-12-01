package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.stat.ScoreTest
import org.dizhang.seqa.util.Constant
import org.dizhang.seqa.util.Constant.{UnPhased, Pheno}
import org.dizhang.seqa.util.InputOutput.VCF
import org.dizhang.seqa.worker.GenotypeLevelQC.{getMaf, makeMaf}
import org.dizhang.seqa.stat.{Regression, LinearRegression, LogisticRegression}
import collection.JavaConverters._
import AsymptoticTest._

/**
  * asymptotic test
  */
object AsymptoticTest {
  /** constants */
  val CPSomeTrait = Constant.ConfigPath.Association.SomeTrait
  val CPTrait = Constant.ConfigPath.Association.`trait`
  val CPTraitList = Constant.ConfigPath.Association.Trait.list
  val CPSomeMethod = Constant.ConfigPath.Association.SomeMethod
  val CPMethod = Constant.ConfigPath.Association.method
  val CPMethodList = Constant.ConfigPath.Association.Method.list
  val CVSomeTrait = Constant.ConfigValue.Association.SomeTrait
  val CVSomeMethod = Constant.ConfigValue.Association.SomeMethod

  def encode(currentGenotype: VCF,
             currentTrait: DenseVector[Double],
             cov: Option[DenseMatrix[Double]],
             controls: Array[Boolean],
             methodConfig: Config)(implicit sc: SparkContext): RDD[(String, Encode)] = {

    /** this is quite tricky
      * some encoding methods require phenotype information
      * e.g. to learning weight from data */
    val sampleSize = currentTrait.length
    val controlsBC = sc.broadcast(controls)
    val yBC = sc.broadcast(currentTrait)
    val covBC = sc.broadcast(cov)
    val codingScheme = methodConfig.getString(CPSomeMethod.coding)
    val mafSource = methodConfig.getString(CPSomeMethod.Maf.source)
    val weightMethod = methodConfig.getString(CPSomeMethod.weight)
    val annotated = codingScheme match {
      case CVSomeMethod.Coding.single => currentGenotype.map(v => (v.pos, v)).groupByKey()
      case _ => currentGenotype.map(v => (v.parseInfo(Constant.Variant.InfoKey.gene), v)).groupByKey()}

    annotated.map(p =>
      (codingScheme, mafSource, weightMethod) match {
        case (CVSomeMethod.Coding.brv, CVSomeMethod.Maf.Source.controls, CVSomeMethod.Weight.erec) =>
          (p._1, Encode(p._2, sampleSize, controlsBC.value, yBC.value, covBC.value, methodConfig))
        case (CVSomeMethod.Coding.brv, _, CVSomeMethod.Weight.erec) =>
          (p._1, Encode(p._2, sampleSize, yBC.value, covBC.value, methodConfig))
        case (_, CVSomeMethod.Maf.Source.controls, _) =>
          (p._1, Encode(p._2, sampleSize, controlsBC.value, methodConfig))
        case (_, _, _) =>
          (p._1, Encode(p._2, sampleSize, methodConfig))})}

  def runTraitWithMethod(currentGenotype: VCF,
                         currentTrait: (String, DenseVector[Double]),
                         cov: Option[DenseMatrix[Double]],
                         controls: Array[Boolean],
                         method: String)(implicit sc: SparkContext, config: Config): Unit = {
    val methodConfig = config.getConfig(s"$CPMethod.$method")
    val coding = encode(currentGenotype, currentTrait._2, cov, controls, methodConfig)
    val yBC = sc.broadcast(currentTrait)
    val covBC = sc.broadcast(cov)
    val fixedMaf = methodConfig.getBoolean(CPSomeMethod.Maf.fixed)
    val weightMethod = methodConfig.getString(CPSomeMethod.weight)
    val permutation = methodConfig.getBoolean(CPSomeMethod.permutation)
    val test = methodConfig.getString(CPSomeMethod.test)
    val binary = config.getBoolean(s"$CPTrait.${currentTrait._1}.${CPSomeTrait.binary}")

    if (permutation) {

    } else {
      val res = (test, fixedMaf) match {
        case (CVSomeMethod.Test.score, true) => {
          val real = coding.map(p => (p._1, p._2.getFixed())).filter(p => p._2.isDefined)
          val covBetas = cov match {
            case None => None
            case _ => Some(if (binary)
                new LogisticRegression(currentTrait._2, cov.get) .estimates
              else
                new LinearRegression(currentTrait._2, cov.get) .estimates)}
          real.map{p =>
            (p._1, ScoreTest(binary, yBC.value._2, p._2.get, covBC.value, covBetas).summary)}}
        case (CVSomeMethod.Test.score, false) => {
          val real = coding.map(p => (p._1, p._2.getVT)).filter(p => p._2.isDefined)
          val covBetas =
            if (binary)
              new LogisticRegression(currentTrait._2, cov.get) .estimates
            else
              new LinearRegression(currentTrait._2, cov.get) .estimates
          real.map(p =>
            (p._1, ScoreTest(binary, yBC.value._2, p._2.get, cov.get, covBetas).summary))
        }
      }
    }
  }
}

class AsymptoticTest(genotype: VCF, phenotype: Phenotype)(implicit config: Config, sc: SparkContext) extends Assoc {
  def runTrait(traitName: String) = {
    try {
      logger.info(s"load trait $traitName from phenotype database")
      val currentTrait = (traitName, phenotype.makeTrait(traitName))
      val methods = config.getStringList(CPMethodList).asScala.toArray
      val indicator = sc.broadcast(phenotype.indicate(traitName))
      val controls = phenotype(Pheno.Header.control).zip(indicator.value)
        .filter(p => p._2).map(p => if (p._1.get == 1.0) true else false)
      val currentGenotype = genotype.map(v => v.select(indicator.value))
      val ConfigPathCov = s"$CPTrait.$traitName.${CPSomeTrait.covariates}"
      val currentCov =
        if (config.hasPath(ConfigPathCov))
          Some(phenotype.makeCov(traitName, config.getStringList(ConfigPathCov).asScala.toArray))
        else
          None
      methods.foreach(m => runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m))
    } catch {
      case e: Exception => logger.warn(e.getStackTrace.toString)
    }
  }
}

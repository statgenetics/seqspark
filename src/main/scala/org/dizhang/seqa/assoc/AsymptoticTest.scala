package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.util.Constant
import org.dizhang.seqa.util.Constant.{UnPhased, Pheno}
import org.dizhang.seqa.util.InputOutput.VCF
import org.dizhang.seqa.worker.GenotypeLevelQC.{getMaf, makeMaf}
import collection.JavaConverters._
import AsymptoticTest._

/**
  * asymptotic test
  */
object AsymptoticTest {
  /** constants */
  val ConfigPathSomeTrait = Constant.ConfigPath.Association.SomeTrait
  val ConfigPathTrait = Constant.ConfigPath.Association.`trait`
  val ConfigPathTraitList = Constant.ConfigPath.Association.Trait.list
  val ConfigPathSomeMethod = Constant.ConfigPath.Association.SomeMethod
  val ConfigPathMethod = Constant.ConfigPath.Association.method
  val ConfigPathMethodList = Constant.ConfigPath.Association.Method.list
  val ConfigValueSomeTrait = Constant.ConfigValue.Association.SomeTrait
  val ConfigValueSomeMethod = Constant.ConfigValue.Association.SomeMethod

}


class AsymptoticTest(genotype: VCF, phenotype: Phenotype)(implicit config: Config, sc: SparkContext) extends Assoc {
  def runTrait(traitName: String) = {
    try {
      logger.info(s"load trait $traitName from phenotype database")
      val currentTrait = phenotype.makeTrait(traitName)
      val methods = config.getStringList(ConfigPathMethodList).asScala.toArray
      val indicator = sc.broadcast(phenotype.indicate(traitName))
      val controls = phenotype(Pheno.Header.control).zip(indicator.value)
        .filter(p => p._2).map(p => if (p._1.get == 1.0) true else false)
      val currentGenotype = genotype.map(v => v.select(indicator.value))
      val ConfigPathCov = s"$ConfigPathTrait.$traitName.${ConfigPathSomeTrait.covariates}"
      val currentCov =
        if (config.hasPath(ConfigPathCov))
          Some(phenotype.makeCov(traitName, config.getStringList(ConfigPathCov).asScala.toArray))
        else
          None
      methods.foreach(m => this.runTraitWithMethod(currentGenotype, currentTrait, currentCov, controls, m))
    } catch {
      case e: Exception => logger.warn(e.getStackTrace.toString)
    }
  }

  def runTraitWithMethod(currentGenotype: VCF,
                         currentTrait: DenseVector[Double],
                         cov: Option[DenseMatrix[Double]],
                         controls: Array[Boolean],
                         method: String): Unit = {
    val methodConfig = config.getConfig(s"$ConfigPathMethod.$method")
    /** this is quite tricky
      * some encoding methods require phenotype information
      * e.g. to learning weight from data */
    val codingScheme = methodConfig.getString(ConfigPathSomeMethod.coding)
    val controlsBC = sc.broadcast(controls)
    val covBC = sc.broadcast(cov)
    val yBC = sc.broadcast(currentTrait)
    if (codingScheme == ConfigValueSomeMethod.Coding.single) {
      val cutoff = methodConfig.getDouble(ConfigPathSomeMethod.Maf.cutoff)
      val coding: RDD[(String, DenseVector[Double])] = currentGenotype.filter{v =>
        methodConfig.getString(ConfigPathSomeMethod.Maf.source) match {
          case ConfigValueSomeMethod.Maf.Source.annotation =>
            v.parseInfo(Constant.Variant.InfoKey.maf).toDouble >= cutoff
          case ConfigValueSomeMethod.Maf.Source.controls =>
            getMaf(v.select(controlsBC.value).toCounter(makeMaf, (0, 2)).reduce) >= cutoff
          case ConfigValueSomeMethod.Maf.Source.pooled =>
            getMaf(v.toCounter(makeMaf, (0, 2)).reduce) >= cutoff
        }
      }.map{v =>
        val maf = getMaf(v.toCounter(makeMaf, (0, 2)).reduce)
        (v.pos, DenseVector(v.map {
          case UnPhased.Bt.ref => 0.0
          case UnPhased.Bt.het1 => 1.0
          case UnPhased.Bt.het2 => 1.0
          case UnPhased.Bt.mut => 2.0
          case _ => 2 * maf
        }.toArray))}

    } else {

      val mafSource = methodConfig.getString(ConfigPathSomeMethod.Maf.source)
      val mafFixed = methodConfig.getBoolean(ConfigPathSomeMethod.Maf.fixed)
      val weightMethod = methodConfig.getString(ConfigPathSomeMethod.weight)
      val sampleSize = currentTrait.length
      val coding: RDD[(String, Encode)] =
        currentGenotype.map(v => (v.parseInfo(Constant.Variant.InfoKey.gene), v))
        .groupByKey().map(p =>
          (codingScheme, mafSource, weightMethod) match {
            case (ConfigValueSomeMethod.Coding.brv, ConfigValueSomeMethod.Maf.Source.controls, ConfigValueSomeMethod.Weight.erec) =>
              (p._1, Encode(p._2, sampleSize, controlsBC.value, yBC.value, covBC.value, methodConfig))
            case (ConfigValueSomeMethod.Coding.brv, _, ConfigValueSomeMethod.Weight.erec) =>
              (p._1, Encode(p._2, sampleSize, yBC.value, covBC.value, methodConfig))
            case (_, ConfigValueSomeMethod.Maf.Source.controls, _) =>
              (p._1, Encode(p._2, sampleSize, controlsBC.value, methodConfig))
            case (_, _, _) =>
              (p._1, Encode(p._2, sampleSize, methodConfig))})

    }
  }
}

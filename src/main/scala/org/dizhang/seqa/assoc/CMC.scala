package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqa.stat.{LogisticRegression, LinearRegression}
import org.dizhang.seqa.util.Constant._
import org.dizhang.seqa.util.InputOutput._
import org.dizhang.seqa.ds._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.dizhang.seqa.worker.GenotypeLevelQC.{makeMaf, getMaf}
import scala.collection.JavaConverters._

/**
 * CMC method for rare variant association
 */

object demo {
  val variants = Array(Array("0/0"))
  def maf(v: Array[String]) = 0.0
  /** Scala code to compute CMC coding
    * suppose variants is a collection
    * of Array of genotypes, maf  */
  def cmc(g: String, maf: Double) =
    g match {
      case "./." =>
        1.0 - math.pow(1.0 - maf, 2)
      case "0/0" => 0.0
      case _ => 1.0
    }
  def compose(p: Double, q: Double) =
    if (p == 0.0 || q == 0.0)
      1.0
    else
      1.0 - (1.0 - p) * (1.0 - q)
  val c = variants
    .map(v => v.map(g => cmc(g, maf(v))))
    .reduce((v1, v2) =>
      (0 until v1.length)
        .map(i => compose(v1(i), v2(i))))
}

object CMC {
  /** Implement (Auer et al. 2013) to make mean imputation on missing genotypes */
  def makeWithNaAdjust(x: Byte, m: Double): Double = {
    if (x == Bt.mis)
      1.0 - math.pow(1.0 - m, 2)
    else
      x.toDouble
  }

  /** use op in this object to reduce CMC codings, in stead of the default plus function */
  object AdderWithNaAdjust extends Counter.CounterElementSemiGroup[Double] {
    def zero = 0.0

    def pow(x: Double, i: Int) = {
      if (x == 1.0)
        1.0
      else if (x == 0.0)
      /** Although it is a case of 'else', add this as fast compute for 0.0
        * Nevertheless, this method 'pow' is not in use right now */
        0.0
      else
        1.0 - math.pow(1.0 - x, i)
    }

    def op(a: Double, b: Double) = {
      if (a == 1.0 || b == 1.0)
        1.0
      else
        1.0 - (1.0 - a) * (1.0 - b)
    }
  }

  def collapse(i: Iterable[Var]): DenseVector[Double] = {
    i.map(v => {
      val maf: Double = getMaf(v.toCounter(makeMaf).reduce)
      v.toCounter(makeWithNaAdjust(_, maf))
    }).reduce((a, b) => a.++(b)(AdderWithNaAdjust).toDenseVector(x => x))
  }
}

class CMC(input: VCF)(implicit val cnf: Config, sc: SparkContext) extends Assoc {

  def runTrait(t: String, funcVars: VCF): Unit = {
    val ped = cnf.getString("sampleInfo.source")
    if (hasColumn(ped, t)) {
      logger.info(s"load trait $t from phenotype file $ped")
      val yAll = readColumn(ped, t)
      val indicator =  yAll.zipWithIndex.map(x => if (x._1 == "NA") false else true)
      val y: DenseVector[Double] = DenseVector(yAll.zip(indicator).filter(x => x._2).map(_._1.toDouble))
      val covList = cnf.getStringList(s"association.trait.$t.covariates").asScala.toList
      val cov: Option[DenseMatrix[Double]] = prepareCov(ped, t, covList, indicator)
      val annot =  cnf.getStringList("association.filter").asScala.toArray
      val xsWithStrKeys = funcVars
        .map(v => (v.parseInfo("ANNO"), v.select(indicator)))
        .groupByKey()
        .map(x => (x._1, CMC.collapse(x._2)))
        .sortByKey()
      xsWithStrKeys.cache()
      val xs = xsWithStrKeys.zipWithUniqueId().map(x => (x._2.toInt, x._1._2))

      if (cnf.getBoolean(s"association.permutation"))
        xs.map(x => {
          val traitType = cnf.getString(s"association.trait.$t.type")
          if (traitType == "binary")
            new LogisticRegression(y, x._2, cov).summary
          else
            new LinearRegression(y, x._2, cov).summary
        })
      else
        new Permutation(y, xs, cov)
    }
    else
      logger.warn(s"no such trait $t in phenotype file $ped. Do nothing.")
  }

  def run {
    val group = "ANNO"
    val filter = cnf.getStringList("association.filter")
    val funcVars = input.filter(v => filter.contains(v.parseInfo(group).split(":").last))
    funcVars.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val traits = cnf.getStringList("association.trait.list").asScala.toList
    traits.foreach(t => runTrait(t, funcVars))
  }

}

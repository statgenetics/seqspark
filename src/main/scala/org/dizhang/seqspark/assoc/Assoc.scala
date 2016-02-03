package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats._
import com.typesafe.config.Config
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.InputOutput._
import org.slf4j.{LoggerFactory, Logger}
import scala.annotation.tailrec


/**
 * Super class for association methods
 */

//val maf = variant.map(v => make(v)).reduce((a, b) => a + b)

abstract class Assoc {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
  *val cnf: Config
  *def prepareCov(ped: String, t: String, covList: List[String], indicator: Array[Boolean]): Option[DenseMatrix[Double]] = {

    *def checkedCov(cl: List[String]): List[String] = {
      *cl.filter(c => hasColumn(ped, c))
    *}

    *def getCov(c: String): DenseVector[Double] = {
      *val raw = readColumn(ped, c).zip(indicator).filter(_._2).map(_._1)
      *val m: Double = mean(raw.filter(_ != Pheno.mis).map(_.toDouble))
      *DenseVector(raw.map(x => if (x == Pheno.mis) m else x.toDouble))
 *
    *}
 *

    *@tailrec def labelsFunc(res: DenseMatrix[Double], cl: List[String]): DenseMatrix[Double] = {
      if (cl == Nil)
        res
      else {
        val cur = DenseMatrix.horzcat(res, getCov(cl.head).toDenseMatrix.t)
        labelsFunc(cur, cl.tail)
      }
    }

    val cc = checkedCov(covList)
    if (cc == Nil) {
      logger.warn(s"No covariate found in ped file $ped")
      None
    } else
      Some(labelsFunc(getCov(cc.head).toDenseMatrix.t, cc.tail))
  }
  */
}

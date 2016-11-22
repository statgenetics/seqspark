package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats._
import com.typesafe.config.Config
import org.dizhang.seqspark.ds.Counter.CounterElementSemiGroup
import org.dizhang.seqspark.ds.Variation
import org.dizhang.seqspark.stat.ScoreTest
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.InputOutput._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec


/**
 * Super class for association methods
 */

@SerialVersionUID(7727260001L)
trait AssocMethod extends Serializable

object AssocMethod {
  class Statistic[A](val value: A) {
    def +(that: Statistic[A])(implicit sg: CounterElementSemiGroup[A]): Statistic[A] = {
      new Statistic[A](sg.op(this.value, that.value))
    }
  }
  @SerialVersionUID(7727260101L)
  trait AnalyticTest extends AssocMethod {
    def pValue: Option[Double]
  }
  @SerialVersionUID(7727260201L)
  trait ResamplingTest extends AssocMethod {
    def pCount: (Int, Int)
  }
  @SerialVersionUID(7727260301L)
  trait Result {
    def vars: Array[Variation]
    def pValue: Option[Double]
  }
  @SerialVersionUID(7727260401L)
  case class AnalyticResult(vars: Array[Variation],
                            statistic: Double,
                            pValue: Option[Double]) extends Result {
    override def toString: String = {
      s"${vars.map(_.toString).mkString(",")}\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}"
    }
  }
  case class ResamplingResult(vars: Array[Variation],
                              refStatistic: Double,
                              pCount: (Int, Int)) extends Result {
    def pValue: Option[Double] = Some(pCount._1/pCount._2.toDouble)
    override def toString: String = {
      s"${vars.map(_.toString).mkString(",")}\t$refStatistic\t${pCount._1},${pCount._2}\t${pValue}"
    }
  }
}
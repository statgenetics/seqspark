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

import breeze.linalg.{DenseMatrix, DenseVector}
import org.dizhang.seqspark.ds.SemiGroup
import org.dizhang.seqspark.ds.Variation


/**
 * Super class for association methods
 */

@SerialVersionUID(7727260001L)
trait AssocMethod extends Serializable

object AssocMethod {
  class Statistic[A](val value: A) {
    def +(that: Statistic[A])(implicit sg: SemiGroup[A]): Statistic[A] = {
      new Statistic[A](sg.op(this.value, that.value))
    }
  }



  /** help presenting the results */
  trait Result {
    def vars: Array[Variation]
    def statistic: Double
    def pValue: Option[Double]
    //def self: Result[A] //to get the implementation object, very fun
    def header: String
  }

  sealed trait ResamplingResult extends Result {
    def pCount: (Int, Int)
    def pValue: Option[Double] = Some(pCount._1/pCount._2.toDouble)
  }

  case class BurdenAnalytic(vars: Array[Variation],
                            statistic: Double,
                            pValue: Option[Double],
                            info: String) extends Result {
    def header = Header.ARH.value
    override def toString: String = {
      s"${vars.length}\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}\t$info"
    }
    def self = this
  }
  case class BurdenResampling(vars: Array[Variation],
                              statistic: Double,
                              pCount: (Int, Int)) extends ResamplingResult {
    def header = Header.RRH.value
    override def toString: String = {
      val pc = pCount
      s"${vars.length}\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}" +
      s"\t${pc._1},${pc._2}"
    }
    def self = this
  }
  case class VTAnalytic(vars: Array[Variation],
                        numTH: Int,
                        statistic: Double,
                        pValue: Option[Double],
                        info: String) extends Result {
    def header = Header.VTAH.value
    override def toString: String = {
      s"${vars.length}\t$numTH\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}" +
      s"\t$info"
    }
    def self = this
  }
  case class VTResampling(vars: Array[Variation],
                          numTH: Int,
                          statistic: Double,
                          pCount: (Int, Int)) extends ResamplingResult {
    def header = Header.VTRH.value
    override def toString: String = {
      val pc = pCount
      s"${vars.length}\t$numTH\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}" +
        s"\t${pc._1},${pc._2}"
    }
    def self = this
  }
  case class SKATResult(vars: Array[Variation],
                        statistic: Double,
                        pValue: Option[Double],
                        info: String) extends Result {
    def header = Header.SKATH.value
    override def toString: String = {
      s"${vars.length}\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}" +
      s"\t$info"
    }
    def self = this
  }
  case class SKATOResult(vars: Array[Variation],
                         pmin: Option[Double],
                         pValue: Option[Double],
                         info: String) extends Result {
    def header = Header.SKATOH.value
    def statistic = 0.0 //hack
    override def toString: String = {
      s"${vars.length}\t${pmin.map(_.toString).getOrElse("NA")}\t${pValue.map(_.toString).getOrElse("NA")}" +
        s"\t$info"
    }
    def self = this
  }

  /** help polymorphism */


  trait Header[A] {
    def value: String
  }

  object Header {
    implicit object ARH extends Header[BurdenAnalytic] {
      def value = "name\tvars\tstatistic\tp-value\tinfo"
    }
    implicit object RRH extends Header[BurdenResampling] {
      def value = "name\tvars\tref-statistic\tp-value\tpermutations"
    }
    implicit object VTAH extends Header[VTAnalytic] {
      def value = "name\tvars\tthresholds\tstatistic\tp-value\tinfo"
    }
    implicit object VTRH extends Header[VTResampling] {
      def value = "name\tvars\tthresholds\tmaxT\tp-value\tpermutations"
    }
    implicit object SKATH extends Header[SKATResult] {
      def value = "name\tvars\tq-score\tp-value\tinfo"
    }
    implicit object SKATOH extends Header[SKATOResult] {
      def value = "name\tvars\tminP\tp-value\tinfo"
    }
  }

  @SerialVersionUID(7727260101L)
  trait AnalyticTest extends AssocMethod
  @SerialVersionUID(7727260201L)
  trait ResamplingTest extends AssocMethod {
    def pCount: (Int, Int)
  }
  /**
  //@SerialVersionUID(7727260301L)
  trait ResultOld {
    def vars: Array[Variation]
    def pValue: Option[Double]
  }
  //@SerialVersionUID(7727260401L)
  case class AnalyticResultOld(vars: Array[Variation],
                               statistic: Double,
                               pValue: Option[Double]) extends ResultOld {
    override def toString: String = {
      s"${vars.map(_.toString).mkString(",")}\t$statistic\t${pValue.map(_.toString).getOrElse("NA")}"
    }
  }
  case class ResamplingResultOld(vars: Array[Variation],
                                 refStatistic: Double,
                                 pCount: (Int, Int)) extends ResultOld {
    def pValue: Option[Double] = Some(pCount._1/pCount._2.toDouble)
    override def toString: String = {
      s"${vars.map(_.toString).mkString(",")}\t$refStatistic\t" +
        s"${pCount._1},${pCount._2}\t${pValue.map(_.toString).getOrElse("NA")}"
    }
  }
  */
}
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

package org.dizhang.seqspark.ds

import breeze.linalg.{DenseMatrix => DM, DenseVector => DV}
import breeze.stats.{mean, stddev}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dizhang.seqspark.ds.Phenotype._
import org.dizhang.seqspark.util.Constant.Pheno
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

 /**
  * Created by zhangdi on 9/13/16.
  */
object Phenotype {

   val logger: Logger = LoggerFactory.getLogger(getClass)
   val options = Map(
     "nullValue" -> Pheno.mis,
     "sep" -> Pheno.delim,
     "header" -> "true"
   )

   def update(path: String, table: String)(spark: SparkSession): Phenotype = {
     logger.debug(s"phenotype path: $path")
     val dataFrame = spark.read.options(options).csv(path)
     logger.info(s"add ${dataFrame.columns.mkString(",")} to phenotype dataframe")
     val old = spark.table(table)
     val newdf = old.join(dataFrame, usingColumn = "iid")
     newdf.createOrReplaceTempView(table)
     Distributed(newdf)
   }

   def select(field: String, table: String)(spark: SparkSession): Phenotype = {
     logger.info(s"select samples that have ${field} values")
     val old = spark.table(table)
     val newdf = spark.sql(s"SELECT * from $table where $field is not null")
     newdf.createOrReplaceTempView(table)
     Distributed(newdf)
   }

   def apply(path: String, table: String)(spark: SparkSession): Phenotype = {

     if (path.isEmpty) {
       Dummy
     } else {
       logger.info(s"creating phenotype dataframe from $path")

       val dataFrame = spark.read.options(options).csv(path)

       dataFrame.createOrReplaceTempView(table)

       Distributed(dataFrame)
     }
  }

   def apply(table: String)(spark: SparkSession): Phenotype = {
     if (spark.catalog.tableExists("phenotype"))
       Distributed(spark.table(table))
     else
       Dummy
   }

   case object Dummy extends Phenotype {
     def select(field: String): Array[Option[String]] = Array[Option[String]]()
     def contains(field: String): Boolean = false
   }

  case class Distributed(df: DataFrame) extends Phenotype {

    private lazy val cols = df.columns

    def contains(field: String): Boolean = cols.contains(field)

    def select(field: String): Array[Option[String]] = {
      if (cols.contains(field)) {
        val spark = df.sparkSession
        import spark.implicits._
        df.select(field).map(r => Option(r(0)).map(_.toString)).collect()

      } else {
        Array.fill(df.count().toInt)(None)
      }
    }
  }

  def winsorize(data: Array[Option[String]], limit: Double): Option[Array[Double]] = {
    val toDouble = Try{data.map(_.map(_.toDouble))} match {
      case Success(s) => s
      case Failure(f) => Array(None)
    }

    if (toDouble.count(_.isDefined) == 0) {
      None
    } else {
      val defined: Array[Double] = toDouble.filter(_.isDefined).map(_.get)
      val Mean: Double = breeze.stats.mean(defined)
      val imputed: Array[Double] = toDouble.map{
        case None => Mean
        case Some(d) => d
      }
      val len: Int = (data.length * limit).toInt
      if (len < 1) {
        Some(imputed)
      } else {
        val sorted: Array[Double] = imputed.sorted
        val (low, up): (Double, Double) = (sorted(len - 1), sorted(sorted.length - len))
        val res: Array[Double] = imputed.map{x =>
          if (x <= low) {
            low
          } else if (x >= up) {
            up
          } else {
            x
          }
        }
        Some(res)
      }
    }
  }
}

trait Phenotype {

  def select(field: String): Array[Option[String]]
  def contains(field: String): Boolean
  def sampleNames: Array[String] = this.select(Pheno.iid).map(_.getOrElse(Pheno.mis))
  def batch(field: String): Option[Array[String]] = {
    val raw = select(field)
    if (raw.flatten.isEmpty) {
      None
    } else {
      Some(raw.map{
        case None => s"$field:None"
        case Some(s) => s
      })
    }
  }
  def batch(fields: Array[String]): Option[Array[String]] = {
    val merged =
      fields.flatMap{f =>
        this.batch(f)
     }
    if (merged.isEmpty)
      None
    else
      Some(merged.reduce((a, b) => a.zip(b).map(p => s"${p._1},${p._2}")))
  }

  def control: Option[Array[Boolean]] = {
    if (this.contains(Pheno.control))
      Some(this.indicate(Pheno.control, Pheno.t))
    else
      None
  }

  def indicate(field: String): Array[Boolean] = {
    this.select(field).map(ov => ov.isDefined)
  }

  def indicate(field: String, value: String): Array[Boolean] = {
    this.select(field).map(ov => ov.isDefined && ov.get == value)
  }

  def getTrait(y: String): Either[String, DV[Double]] = {
    val defined = this.select(y).filter(_.isDefined).map(_.get)
    if (defined.isEmpty) {
      Left(s"No valid phenotype for column $y")
    } else {
      Try{defined.map(_.toDouble)} match {
        case Success(s) => Right(DV(s))
        case Failure(f) => Left(f.getMessage)
      }
    }
  }
  def getCov(y: String, cov: Array[String], limits: Array[Double]): Either[String, DM[Double]] = {
    val indicator = this.indicate(y)
    if (indicator.count(_ == true) == 0) {
      Left(s"No valid phenotype for column $y ")
    } else {
      val res = cov.zip(limits).map{
        case (c, limit) =>
          val raw = this.select(c).zip(indicator).filter(_._2).map(_._1)
          val v = winsorize(raw, limit)
          if (v.isEmpty) logger.warn(s"$c not available")
          v
      }
      if (res.exists(_.isDefined)) {
        val scaled = DV.horzcat(res.filter(_.isDefined).map{x =>
          val u = DV(x.get)
          (u - mean(u))/stddev(u)
        }: _*)
        Right(scaled)
      } else {
        val msg = cov.zip(res).filter(p => p._2.isEmpty).map(p => s"invalid value in covariate ${p._1}")
        Left(msg.mkString(";"))
      }
    }
  }

}

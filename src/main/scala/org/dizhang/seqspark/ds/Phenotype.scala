package org.dizhang.seqspark.ds

import breeze.linalg.{DenseMatrix => DM, DenseVector => DV}
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

   def update(path: String, table: String)(spark: SparkSession): Phenotype = {
     val options = Map(
       "nullValue" -> Pheno.mis,
       "sep" -> Pheno.delim,
       "header" -> "true"
     )
     val dataFrame = spark.read.options(options).csv(path)
     logger.info(s"add ${dataFrame.columns.mkString(",")} to phenotype dataframe")
     val old = spark.table(table)
     val newdf = old.join(dataFrame, usingColumn = "iid")
     newdf.createOrReplaceTempView(table)
     Distributed(newdf)
   }

  def apply(path: String, table: String)(spark: SparkSession): Phenotype = {

    val options = Map(
      "nullValue" -> Pheno.mis,
      "sep" -> Pheno.delim,
      "header" -> "true"
    )

    val dataFrame = spark.read.options(options).csv(path)

    logger.info(s"create phenotype dataframe from $path")

    dataFrame.createOrReplaceTempView(table)

    Distributed(dataFrame)
  }

   def apply(table: String)(spark: SparkSession): Phenotype = {
     Distributed(spark.table(table))
   }


  case class Distributed(df: DataFrame) extends Phenotype {

    private lazy val cols = df.columns

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
  def sampleNames = this.select("iid").map(_.getOrElse("NA"))
  def batch(field: String): Array[String] = {
    this.select(field).map{
      case None => s"$field:None"
      case Some(s) => s
    }
  }
  def batch(fields: Array[String]): Array[String] = {
    fields.map{ f =>
      this.batch(f)
    }.reduce((a, b) => a.zip(b).map(p => s"${p._1},${p._2}"))
  }
  def indicate(field: String): Array[Boolean] = {
    this.select(field).map{
      case None => false
      case Some(s) => true
    }
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
          winsorize(raw, limit)
      }
      if (res.forall(_.isDefined)) {
        Right(DV.horzcat(res.map(x => DV(x.get)): _*))
      } else {
        val msg = cov.zip(res).filter(p => p._2.isEmpty).map(p => s"invalid value in covariate ${p._1}")
        Left(msg.mkString(";"))
      }
    }
  }

}

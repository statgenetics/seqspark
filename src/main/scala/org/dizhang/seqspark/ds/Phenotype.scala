package org.dizhang.seqspark.ds

import breeze.linalg.{DenseMatrix => DM, DenseVector => DV, _}
import breeze.stats._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dizhang.seqspark.util.Constant.Pheno
import scala.util.{Failure, Success, Try}
import Phenotype._

 /**
  * Created by zhangdi on 9/13/16.
  */
object Phenotype {

  def apply(path: String, sc: SparkContext): Phenotype = {
    //val sqlContext = new SQLContext(sc)

    val spark = SparkSession
      .builder()
      .appName("SeqSpark Phenotype")
      .getOrCreate()

    val options = Map(
      "nullValue" -> Pheno.mis,
      "sep" -> Pheno.delim,
      "header" -> "true"
    )

    val dataFrame = spark.read.options(options).csv(path)
    /**
    val raw = sc.textFile(path).cache()
    val head = raw.first()
    val ped = raw.zipWithUniqueId().filter(_._2 != 0).map(_._1)

    val scheme =
      StructType(
        head.split(Pheno.delim)
        .map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val rowRDD = ped.map{l =>
      val p = l.split(Pheno.delim)
      Row.fromSeq(p.map{
        case Pheno.mis => null
        case x => x
      })
    }
    val dataFrame = sqlContext.createDataFrame(rowRDD, scheme)
    */
    Distributed(dataFrame)
  }

  case class Distributed(private val df: DataFrame) extends Phenotype {
    def select(field: String): Array[Option[String]] = {
      if (df.columns.contains(field)) {
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
      val defined = DV(toDouble.filter(_.isDefined).map(_.get))
      val Mean = mean(defined)
      val imputed = toDouble.map{
        case None => Mean
        case Some(d) => d
      }
      val len = (data.length * limit).toInt
      if (len < 1) {
        Some(imputed)
      } else {
        val sorted = imputed.sorted
        val (low, up) = (sorted(len - 1), sorted(sorted.length - len))
        val res = imputed.map{x =>
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

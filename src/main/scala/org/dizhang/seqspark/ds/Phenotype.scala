package org.dizhang.seqspark.ds

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.dizhang.seqspark.util.Constant.Pheno

import scala.io.Source

/**
  * Phenotype is a SparkSQL database
  */
object Phenotype {
  def apply(file: String)(implicit sc: SparkContext): Phenotype = {
    val sqlContext = new SQLContext(sc)
    val scheme =
      StructType(
        Source.fromFile(file).getLines().next().split(Pheno.delim)
          .map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val rowRDD = sc.textFile(s"file://$file").map(_.split(Pheno.delim)).map(p => Row(p: _*))
    val dataFrame = sqlContext.createDataFrame(rowRDD, scheme)
    dataFrame.registerTempTable("sampleInfo")
    new Phenotype(dataFrame)
  }

  def save(pheno: Phenotype, path: String): Unit = {
    pheno.dataFrame.write.save(path)
  }

}

class Phenotype(val dataFrame: DataFrame) {
  def apply(field: String): Array[Option[Double]] = {
    this.dataFrame.select(field).map{case null => None; case s => Some(s.toString.toDouble)}.collect()
  }
  def batch: Array[String] = {
    this.dataFrame.select("batch").map{case null => "1"; case s => s.toString()}.collect()
  }
  def filter(field: String): Phenotype = {
    new Phenotype(dataFrame.filter(s"$field == 1"))
  }
  def indicate(field: String): Array[Boolean] = {
    this.dataFrame.select(field).map{case null => false; case s => true}.collect()
  }
  def makeTrait(field: String): DenseVector[Double] = {
    DenseVector(this(field).filter(x => x.isDefined).map(x => x.get))
  }
  def makeCov(y: String, cov: Array[String])(implicit sc: SparkContext): DenseMatrix[Double] = {
    val indicator = sc.broadcast(this.indicate(y))
    val mat = cov.map{ c =>
      val ao = this(c).zip(indicator.value).filter(p => p._2).map(p => p._1)
      val av = mean(ao.filter(p => p.isDefined).map(_.get))
      ao.map{case None => av; case Some(d) => d}
    }
    DenseMatrix(mat: _*).t
  }
}
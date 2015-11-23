package org.dizhang.seqa.ds

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.dizhang.seqa.util.Constant.Pheno
import scala.io.Source

/**
  * Phenotype is a SparkSQL database
  */
object Phenotype {
  def apply(file: String)(implicit sc: SparkContext): Unit = {
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
}

class Phenotype(val dataFrame: DataFrame) {
  def apply(field: String): DataFrame = {
    this.dataFrame.select(field)
  }
}
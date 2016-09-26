package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.feature.{PCA => SPCA}
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.worker.Data
import org.dizhang.seqspark.geno.Genotype
import org.dizhang.seqspark.util.General._

/**
  * perform PCA for
  */
class PCA[A](vcf: Data[A])(implicit geno: Genotype[A]) {

  def transpose: RDD[Vector] = {
    val input = vcf
    val byColAndRow = input.zipWithIndex().flatMap{
      case (v, ri) =>
        val maf = v.toCounter(geno.maf, (0.0, 2.0)).reduce.ratio
        v.toIndexedSeq.zipWithIndex.map{
        case (g, ci) => ci -> (ri, geno.toBRV(g, maf))
      }
    }
    val byCol = byColAndRow.groupByKey().sortByKey().values
    val res = byCol.map {
      row => Vectors.dense(row.toArray.sortBy(_._1).map(_._2))
    }
    res
  }
  def pc(n: Int): BDM[Double] = {
    val model = new SPCA(n)
    val data = this.transpose
    val res = model.fit(data).pc.values
    new BDM(res.length/n, n, res)
  }
}
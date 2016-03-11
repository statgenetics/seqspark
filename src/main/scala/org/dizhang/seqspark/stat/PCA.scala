package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.feature.{PCA => SPCA}
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.VCF

/**
  * perform PCA for
  */
class PCA(val vcf: VCF) {
  def transpose: RDD[Vector] = {
    val input = vcf.vars
    val byColAndRow = input.zipWithIndex().flatMap{
      case (v, ri) => v.toArray.zipWithIndex.map{
        case (g, ci) => ci -> (ri, g.toDouble)
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


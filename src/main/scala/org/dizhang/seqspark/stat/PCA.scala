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

package org.dizhang.seqspark.stat

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.spark.mllib.feature.{PCA => SPCA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{DenseCounter, Genotype, SparseCounter}
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.worker.Data
import org.slf4j.LoggerFactory

/**
  * perform PCA for
  */
@SerialVersionUID(103L)
class PCA(input: RDD[Array[Double]]) extends Serializable {

  val logger = LoggerFactory.getLogger(getClass)

  def prepare: RDD[Vector] = {

    if (input.isEmpty()) {
      logger.warn("no input for PCA")
    }

    input.map(a => Vectors.dense(a))

    /**
      * val input = vcf
    val byColAndRow = input.zipWithIndex().flatMap{
      case (v, ri) =>
        val maf = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce.ratio
        v.toIndexedSeq.zipWithIndex.map{
        case (g, ci) => ci -> (ri, geno.toBRV(g, maf))
      }
    }
    val byCol = byColAndRow.groupByKey().sortByKey().values
    val res = byCol.map {
      row => Vectors.dense(row.toArray.sortBy(_._1).map(_._2))
    }

    res.cache()
    logger.info(s"PCA transposed data dimension: ${res.count()} x ${res.first().size}")
    res
      */
  }
  def pc(n: Int): BDM[Double] = {
    val model = new SPCA(n)
    val data = this.prepare
    if (data.isEmpty()) {
      new BDM[Double](0, 0)
    } else {
      val res = model.fit(data).pc.values
      new BDM(res.length/n, n, res)
    }
  }
}
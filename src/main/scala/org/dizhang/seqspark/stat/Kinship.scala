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

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds._
import breeze.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangdi on 10/11/17.
  *
  * Methods to calculate kinship matrix
  *
  */



abstract class Kinship[A: Genotype] {
  def geno: RDD[Variant[A]]
  def coef: RDD[((Int, Int), Double)]
  def related: RDD[((Int, Int), Int)]
}

object Kinship {

  /**
    * Key: individual pair
    * Cnt4: counter
    * */
  type Key = (Int, Int)
  type Cnt4 = (Int, Int, Int, Int)

  case class KingRobust[A: Genotype](geno: RDD[Variant[A]],
                                     pedigree: Pedigree)
                                    (implicit sc: SparkContext) extends Kinship {
    lazy val coef: RDD[((Int, Int), Double)] = {
      val bcPed = sc.broadcast(pedigree)
      geno.flatMap(v =>
        kingRobust(v)).reduceByKey((a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
      ).map {
        case ((i, j), (het2, hom2, hetI, hetJ)) =>
          val c = if (bcPed.value.data(i).fid == bcPed.value.data(j).fid) {
            (het2 - 2 * hom2) / (hetI + hetJ).toDouble
          } else {
            val hetM = Math.min(hetI, hetJ)
            (het2 - 2 * hom2) / (2 * hetM).toDouble + 0.5 - (hetI + hetJ) / (4 * hetM).toDouble
          }
          ((i, j), c)
      }
    }

    def related: RDD[((Int, Int), Int)] = {
      coef.filter(p => p._2 > math.pow(2, -4.5)).map{
        case (k, c) =>
          if (c > math.pow(2, -1.5))
            (k, 0)
          else if (c > math.pow(2, -2.5))
            (k, 1)
          else if (c > math.pow(2, -3.5))
            (k, 2)
          else
            (k, 3)
      }
    }

  }

  /**
    * King Robust
    *
    * Count N_{Aa,Aa}, N_{AA,aa}, N_{i}{Aa}, N_{j}{Aa}
    *
    * */
  def kingRobust[A: Genotype](variant: Variant[A]): Iterator[(Key, Cnt4)] = {
    val geno = implicitly[Genotype[A]]
    val het = getIndexByGeno(variant, geno.isHet)
    val ref = getIndexByGeno(variant, geno.isRef)
    val mut = getIndexByGeno(variant, geno.isMut)

    val res1: Iterator[(Key, Cnt4)] = for {
      i<- het.toIterator
      j<- het.toIterator
      if j < i
    } yield ((i,j), (1, 0, 1, 1))

    val res2 = for {
      i <- ref.toIterator
      j <- mut.toIterator
    } yield {
      if (i < j) ((i, j), (0, 1, 0, 0)) else ((j, i), (0, 1, 0, 0))
    }

    res1 ++ res2
  }


  /**
    * get sample indices by testing genotype
    *
    * */
  def getIndexByGeno[A](variant: Variant[A], f: A => Boolean): IndexedSeq[Int] = {
    variant match {
      case DenseVariant(_, elems, _, _) =>
        elems.zipWithIndex.filter(p => f(p._1)).map(_._2)
      case SparseVariant(_, elems, d, n) =>
        val withDefault =
          if (f(d))
           removeNums(n, elems.keys.toIndexedSeq.sorted)
          else
            IndexedSeq[Int]()
        elems.toIndexedSeq.filter(p => f(p._2)).map(_._1) ++ withDefault

      case _ =>
        IndexedSeq[Int]()
    }
  }

  /**
    * removeNums takes a Range(0, size),
    * and removes a subset of numbers from the Range.
    *
    * The numbers in nums are required to be smaller than size.
    * */
  def removeNums(size: Int, nums: IndexedSeq[Int]): IndexedSeq[Int] = {
    var j: Int = 0
    var i: Int = 0
    val res = ArrayBuffer[Int]()
    while (i < size) {
      if (j >= nums.length) {
        res.+=(i)
      } else if (i == nums(j)) {
        j += 1
      } else {
        res.+=(i)
      }
      i += 1
    }
    res.toIndexedSeq
  }

}
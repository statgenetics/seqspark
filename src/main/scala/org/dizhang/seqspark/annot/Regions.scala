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

package org.dizhang.seqspark.annot

import org.apache.spark.SparkContext
import org.dizhang.seqspark.ds.Region
import org.slf4j.LoggerFactory

/**
  * Exome regions implemented with interval trees
  */
class Regions(private val loci: Map[Byte, IntervalTree[Region]]) {

  def count(): Int = {
    loci.map{case (k, v) => IntervalTree.count(v)}.sum
  }

  def overlap(r: Region): Boolean = {
    loci.contains(r.chr) && IntervalTree.overlap(loci(r.chr), r)
  }
  def lookup(r: Region): List[Region] = {
    if (! loci.contains(r.chr)) {
      List[Region]()
    } else {
      IntervalTree.lookup(loci(r.chr), r)
    }
  }
}

object Regions {
  type LOCI = Map[Byte, Array[Region]]

  val logger = LoggerFactory.getLogger(this.getClass)

  def comop(m1: LOCI, m2: LOCI): LOCI = {
    m1 ++ (for ((k, v) <- m2) yield k -> (v ++ m1.getOrElse(k, Array())))
  }

  def apply(raw: Iterator[Region]): Regions = {
    val regArr = raw.toArray

    logger.info(s"${regArr.length} regions to parse")

    val regByChrEle = regArr.map(r => Map(r.chr -> Array(r)))
    //logger.info(s"${regByChrEle.count()} regions after map")
    val regByChr = regByChrEle.reduce((a, b) => comop(a, b))
    //logger.info(s"${regByChr.map{case (k, v) => v.length}.sum} regions after combine")
    val rs = new Regions(regByChr.map{case (k, v) => k -> IntervalTree(v.toIterator)})
    logger.info(s"${rs.count()} regions generated")
    rs
  }

  def makeExome(coordFile: String)(sc: SparkContext): Regions = {

    val locRaw = sc.textFile(coordFile).cache()
    val header = locRaw.first().split("\t")
    val locRdd = locRaw.zipWithUniqueId().filter(_._2 > 0).map(_._1)
    //val iter = scala.io.Source.fromFile(coordFile).getLines()
    val raw = locRdd.filter(l => ! l.split("\t")(2).contains("_"))
      .flatMap(l => RefGene.makeExons(l, header)).toLocalIterator
    apply(raw)
  }
}
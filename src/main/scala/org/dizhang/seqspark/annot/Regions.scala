package org.dizhang.seqspark.annot

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.dizhang.seqspark.ds.Region
import it.unimi.dsi.fastutil.bytes.{Byte2ObjectArrayMap => B2M}

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

  def makeExome(coordFile: String): Regions = {
    val iter = scala.io.Source.fromFile(coordFile).getLines()
    val header = iter.next().split("\t")
    val raw = iter.filterNot(l => l.split("\t")(2).contains("_"))
      .map(l => RefGene.makeExons(l, header))
      .flatten
    apply(raw)
  }
}
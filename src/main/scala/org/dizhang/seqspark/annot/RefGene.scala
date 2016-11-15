package org.dizhang.seqspark.annot


import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.dizhang.seqspark.ds.{Region, Single}
import org.dizhang.seqspark.annot.NucleicAcid._

import scala.io.Source
import org.slf4j.{Logger, LoggerFactory}

/**
  * refgene
  */

object RefGene {

  val logger = LoggerFactory.getLogger(this.getClass)

  def makeLocation(line: String, header: Array[String]): Location = {
    val s = line.split("\t")
    val m = header.zip(s).toMap
    val geneName = m("geneName")
    val mRNAName = m("name")
    val strand = if (m("strand") == "+") Location.Strand.Positive else Location.Strand.Negative
    val t = s"${m(s"chrom")}:${m("cdsStart")}-${m("cdsEnd")}"
    //println(t)
    val cds = Region(t)
    val exons = m("exonStarts").split(",").zip(m("exonEnds").split(",")).map(e => Region(s"${cds.chr}:${e._1}-${e._2}"))
    Location(geneName, mRNAName, strand, exons, cds)
  }

  def makeExons(line: String, header: Array[String]): Array[Region] = {
    //println(line)
    val s = line.split("\t")
    val m = header.zip(s).toMap
    val exons = m("exonStarts").split(",").zip(m("exonEnds").split(","))
      .map{ e => val t = s"${m("chrom")}:${e._1.toInt - 2}-${e._2.toInt + 2}"; Region(t)}
    exons
  }

  def apply(build: String, coordFile: String, seqFile: String)(implicit sc: SparkContext): RefGene = {

    logger.info(s"load RefSeq: coord: $coordFile seq: $seqFile")

    val locRaw = sc.textFile(coordFile)
    //locRaw.cache()
    val header = locRaw.first().split("\t")

    val locRdd = locRaw.zipWithUniqueId().filter(_._2 > 0).map(_._1)
    val loci = IntervalTree(locRdd.map(l => makeLocation(l, header)).toLocalIterator)
    val seqName = """>(\w+_\d+)\.\d+""".r
    val seqLines = sc.textFile(seqFile)

    val seq2 = seqLines.map{
      case seqName(n) => Array((n, ""))
      case l => Array(("", l))
    }

    val seq3 = seq2.mapPartitions{p =>
      p.fold(Array()){(a, b) =>
        if (a.isEmpty) {
          b
        } else if (b.isEmpty) {
          a
        } else if (b.head._1 != "") {
          a ++ b
        } else {
          (a.take(a.length - 1) :+ (a.last._1, a.last._2 + b.head._2)) ++ b.drop(1)
        }
      }.toIterator
    }.collect().map(x => Array(x)).fold(Array()){(a, b) =>
      if (a.isEmpty) {
        b
      } else if (b.isEmpty) {
        a
      } else if (b.head._1 != "") {
        a ++ b
      } else {
        (a.take(a.length - 1) :+ (a.last._1, a.last._2 + b.head._2)) ++ b.drop(1)
      }
    }.toMap

    val seq = seq2.fold(Array()){(a, b) =>
      if (a.isEmpty) {
        b
      } else if (b.isEmpty) {
        a
      } else if (b.head._1 != "") {
        a ++ b
      } else {
        (a.take(a.length - 1) :+ (a.last._1, a.last._2 + b.head._2)) ++ b.drop(1)
      }
    }.map(s => (s._1, makeRNA(s._1, s._2))).toMap


    logger.debug(s"${seq.take(100).keys.mkString(":")}")
    logger.info(s"${seq.size} ${seq3.size} transcript sequences")

    val names = seq.keys

    val pw = new PrintWriter(new File("output/test.seq"))
    //pw.write(s"${seq("").toString}\n")
    for (k <- names) {
      val s = seq3(k)
      pw.write(s"$k: ${s.length}\n")
    }
    pw.close()

    val res = new RefGene(build, loci, seq)
    logger.info(s"${IntervalTree.count(res.loci)} locations generated")

    /**
    val test = IntervalTree.lookup(res.loci, Single(1, 1296690))

    test match {
      case Nil => logger.info("cannot find anything for chr1:1296691")
      case _ => logger.info(s"here we go: ${test.last.toString}")
    }
      */
    res
  }
}

class RefGene(val build: String,
              val loci: IntervalTree[Location],
              val seq: Map[String, mRNA])
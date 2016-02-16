package org.dizhang.seqspark.annot


import org.dizhang.seqspark.ds.Region
import scala.io.Source

/**
  * refgene
  */

object RefGene {
  def makeLocation(line: String, header: Array[String]): Location = {
    val s = line.split("\t")
    val m = header.zip(s).toMap
    val geneName = m("geneName")
    val mRNAName = m("name")
    val strand = if (m("strand") == "+") Location.Strand.Positive else Location.Strand.Negative
    val cds = Region(s"${m(s"chrom")}:${m("cdsStart")}-${m("cdsEnd")}")
    val exons = m("exonStarts").split(",").zip(m("exonEnds").split(",")).map(e => Region(s"${cds.chr}:${e._1}-${e._2}"))
    new Location(geneName, mRNAName, strand, exons, cds)
  }

  def makeSeq(name: String, rawSeq: String): mRNA = {
    val len = rawSeq.length
    val na = rawSeq.zipWithIndex.filter(p => p._1 == 'N').map(p => p._2).toArray
    val m = Map[Char, Byte](
      'T' -> 0,
      't' -> 0,
      'C' -> 1,
      'c' -> 1,
      'A' -> 2,
      'a' -> 2,
      'G' -> 3,
      'g' -> 3,
      'N' -> 0,
      'n' -> 0
    )
    val seq = (for (i <- 0 to len%4) yield {
      val first = m(rawSeq(4*i)) << 6
      val second = if (4*i + 1 < len) m(rawSeq(4*i + 1)) << 4 else 0
      val third = if (4*i + 2 < len) m(rawSeq(4*i + 2)) << 2 else 0
      val forth = if (4*i + 3 < len) m(rawSeq(4*i + 3)) else 0
      (first + second + third + forth).toByte
    }).toArray
    new mRNA(name, len, seq, na)
  }

  def apply(build: String, coordFile: String, seqFile: String): RefGene = {
    val locIter = Source.fromFile(coordFile).getLines()
    val header = locIter.next().split("\t")
    val loci = IntervalTree(locIter.map(l => makeLocation(l, header)))
    val seqIter = Source.fromFile(seqFile).getLines()
    val seqName = """>(NM_\d+).\d""".r

    val seq = seqIter.map{
      case seqName(n) => Array((n + "\t", ""))
      case l => Array(("", l))
    }.reduce{(a, b) =>
      if (b(0)._1.startsWith("NM_")) {
        a ++ b
      } else {
        a(a.length - 1) = (a.last._1, a.last._2 + b(0)._2)
        a ++ b.slice(1, b.length)
      }
    }.map(s => (s._1, makeSeq(s._1, s._2))).toMap

    new RefGene(build, loci, seq)
  }
}

class RefGene(val build: String,
              val loci: IntervalTree[Location],
              val seq: Map[String, mRNA])
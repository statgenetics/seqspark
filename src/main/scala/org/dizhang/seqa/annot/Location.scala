package org.dizhang.seqa.annot

import breeze.linalg.sum
import org.dizhang.seqa.ds.{Region, Single}
import org.dizhang.seqa.util.Constant.Annotation
import org.dizhang.seqa.util.Constant.Annotation.Nucleotide.Nucleotide
import org.dizhang.seqa.util.Constant.Annotation._
import Location._
/**
  * location of a gene with a specific splicing and translation.
  * the cds and utr defined here are not accurate, for it may include part of intronic region.
  * However, it is easier this way to compute.
  */
object Location {
  val feature = Annotation.Feature
  object Strand extends Enumeration {
    type Strand = Value
    val Positive = Value("+")
    val Negative = Value("-")
  }
  val upDownStreamRange = 2000

}

class Location(val geneName: String,
               val mRNAName: String,
               val strand: Strand.Strand,
               val exons: Array[Region],
               val cds: Region) extends Region {

  def chr = cds.chr
  def start = exons(0).start
  def end = exons.last.end

  def upstream: Region = {
    if (strand == Strand.Positive) {
      Region(chr, start - upDownStreamRange, start)
    } else {
      Region(chr, end, end + upDownStreamRange)
    }
  }

  def downStream: Region = {
    if (strand == Strand.Negative) {
      Region(chr, start - upDownStreamRange, start)
    } else {
      Region(chr, end, end + upDownStreamRange)
    }
  }

  def utr5: Region = {
    if (strand == Strand.Positive) {
      Region(chr, start, cds.start)
    } else {
      Region(chr, cds.end, end)
    }
  }

  def utr3: Region = {
    if (strand == Strand.Negative) {
      Region(chr, start, cds.start)
    } else {
      Region(chr, cds.end, end)
    }
  }

  def spliceSites: Array[Region] = {
    val l = exons.length
    if (l == 1) {
      Array[Region]()
    } else {
      val all = exons.slice(0, l - 1).map(e => Region(e.chr, e.end, e.end + 2)) ++
        exons.slice(1, l).map(e => Region(e.chr, e.start - 2, e.start))
      all.sorted
    }
  }

  def codon(p: Single, seq: mRNA, alt: Option[Nucleotide] = None): String = {
    /** find the exon that contains the point */
    val exonIdx = exons.zipWithIndex.filter(e => p overlap e._1)(0)._2
    val idx =
      if (strand == Strand.Positive) {
        val intronsLen =
          sum(for (i <- 0 until exonIdx) yield exons(i + 1).start - exons(i).end)
        p.pos - start - intronsLen
      } else {
        val intronsLen =
          sum(for (i <- exonIdx until exons.length) yield exons(i + 1).start - exons(i).end)
        end - intronsLen - p.pos
      }
    (idx % 3, alt) match {
      case (0, None) => s"${seq(idx)}${seq(idx + 1)}${seq(idx + 2)}"
      case (1, None) => s"${seq(idx - 1)}${seq(idx)}${seq(idx + 1)}"
      case (_, None) => s"${seq(idx - 2)}${seq(idx - 1)}${seq(idx)}"
      case (0, Some(n)) => s"$n${seq(idx + 1)}${seq(idx + 2)}"
      case (1, Some(n)) => s"${seq(idx - 1)}$n${seq(idx + 1)}"
      case (_, Some(n)) => s"${seq(idx - 2)}${seq(idx - 1)}$n"
    }
  }

  def annotate(p: Single, seq: Option[mRNA] = None, alt: Option[Nucleotide] = None): feature.Feature = {
    import AminoAcid._
    if (p overlap upstream) {
      feature.Upstream
    } else if (p overlap downStream) {
      feature.Downstream
    } else if (p overlap this) {
      if (exons.exists(e => p overlap e)) {
        if (p overlap cds) {
          seq match {
            case None => feature.CDS
            case Some(s) =>
              val o = translate(codon(p, s))
              val a = translate(codon(p, s, alt))
              if (o == a) {
                feature.Synonymous
              } else if (o == Stop && a != Stop) {
                feature.StopLoss
              } else if (o != Stop && a == Stop) {
                feature.StopGain
              } else {
                feature.NonSynonymous
              }
          }
        } else {
          if (p overlap utr5) {
            feature.UTR5
          } else {
            feature.UTR3
          }
        }
      } else if (spliceSites.exists(s => p overlap s)) {
        feature.SpliceSite
      } else {
        feature.Intronic
      }
    } else {
      feature.InterGenic
    }
  }
}

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

import breeze.linalg.sum
import org.dizhang.seqspark.annot.Location.Strand.Strand
import org.dizhang.seqspark.annot.Location._
import org.dizhang.seqspark.annot.NucleicAcid._
import org.dizhang.seqspark.ds.{Region, Single, Variation}
import org.dizhang.seqspark.util.Constant.Annotation
import org.dizhang.seqspark.util.Constant.Annotation.Base.Base
import org.dizhang.seqspark.util.Constant.Annotation._
import org.dizhang.seqspark.util.ConfigValue.MutType

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

  def apply(geneName: String,
            mRNAName: String,
            strand: Strand,
            exons: Array[Region],
            cds: Region): Location = {
    strand match {
      case Strand.Negative => NLoc(geneName, mRNAName, exons, cds)
      case _ => PLoc(geneName, mRNAName, exons, cds)
    }
  }

  def totalLen[A <: Region](regs: Array[A]): Int = {
    sum(regs.map(r => r.length))
  }

  def intersects(regs: Array[Region], reg: Region): Array[Region] = {
    regs.filter(r => r overlap reg).map(r => r intersect reg)
  }
}

final case class PLoc(geneName: String,
                      mRNAName: String,
                      exons: Array[Region],
                      cds: Region) extends Location {
  val strand = Location.Strand.Positive
  def upstream = Region(chr, start - upDownStreamRange, start)
  def downstream = Region(chr, end, end + upDownStreamRange)
  def utr5 = Region(chr, start, cds.start)
  def utr3 = Region(chr, cds.end, end)
  def codon(p: Single, seq: mRNA, alt: Option[Base] = None): Codon = {
    val exonIdx = exons.zipWithIndex.filter(e => p overlap e._1).head._2
    val intronsLen =
      sum(for (i <- 0 until exonIdx)
          yield exons(i + 1).start - exons(i).end)
    val idx = p.pos - start - intronsLen
    seq.getCodon(idx, cdsIdx, alt)
  }
}

final case class NLoc(geneName: String,
                      mRNAName: String,
                      exons: Array[Region],
                      cds: Region) extends Location {
  val strand = Location.Strand.Negative
  def upstream = Region(chr, end, end + upDownStreamRange)
  def downstream = Region(chr, start - upDownStreamRange, start)
  def utr5 = Region(chr, cds.end, end)
  def utr3 = Region(chr, start, cds.start)
  def codon(p: Single, seq: mRNA, alt: Option[Base] = None): Codon = {
    val exonIdx = exons.zipWithIndex.filter(e => p overlap e._1).head._2
    val intronsLen =
      sum(for (i <- exonIdx until (exons.length - 1))
          yield exons(i + 1).start - exons(i).end)
    val idx = end - p.pos - intronsLen - 1
    seq.getCodon(idx, cdsIdx, alt)
  }
}

@SerialVersionUID(7726)
sealed trait Location extends Region {

  val geneName: String
  val mRNAName: String
  val strand: Strand.Strand
  val exons: Array[Region]
  val cds: Region

  val chr = cds.chr
  val start = exons(0).start
  val end = exons.last.end

  def upstream: Region

  def downstream: Region

  def utr5: Region

  def utr3: Region

  def ==(that: Location): Boolean = {
    this.geneName == that.geneName &&
    this.mRNAName == that.mRNAName &&
    this.strand == that.strand &&
    this.chr == that.chr &&
    this.start == that.start &&
    this.end == that.end &&
    this.cds == that.cds &&
    this.exons.length == that.exons.length &&
    this.exons.zip(that.exons).forall(p => p._1 == p._2)
  }

  def cdsIdx: (Int, Int) = {
    /** the indexes here are local, only work in mRNA/cDNA sequence */
    val start = totalLen(intersects(exons, utr5))
    val end = totalLen(exons) - totalLen(intersects(exons, utr3))
    (start, end)
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

  def codon(p: Single, seq: mRNA, alt: Option[Base] = None): Codon

  def annotate(v: Variation, seq: mRNA): feature.Feature = {
    v.mutType match {
      case MutType.snv => annotate(Single(v.chr, v.pos), Some(seq), Some(Base.withName(v.alt)))
      case MutType.indel => {
        val int = Region(v.chr, v.pos + 1, v.end)
        if (int overlap this) {
          if (exons.exists(e => e overlap int)) {
            if (int overlap cds) {
              if (math.abs(v.ref.length - v.alt.length)%3 == 0) {
                feature.FrameShiftIndel
              } else {
                feature.NonFrameShiftIndel
              }
            } else if (int overlap utr5) {
              feature.UTR5
            } else {
             feature.UTR3
            }
          } else if (spliceSites.exists(s => s overlap int)) {
            feature.SpliceSite
          } else {
            feature.Intronic
          }
        } else if (int overlap upstream) {
          feature.Upstream
        } else if (int overlap downstream) {
          feature.Downstream
        } else {
          feature.InterGenic
        }
      }
      case MutType.cnv => feature.CNV
    }
  }

  def annotate(p: Single, seq: Option[mRNA] = None, alt: Option[Base] = None): feature.Feature = {
    import AminoAcid._
    if (p overlap upstream) {
      feature.Upstream
    } else if (p overlap downstream) {
      feature.Downstream
    } else if (p overlap this) {
      if (exons.exists(e => p overlap e)) {
        if (p overlap cds) {
          seq match {
            case None => feature.CDS
            case Some(s) =>
              val o = codon(p, s).translate
              val a = codon(p, s, alt).translate
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

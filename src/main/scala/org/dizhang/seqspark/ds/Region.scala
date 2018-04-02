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

package org.dizhang.seqspark.ds

import breeze.linalg.{max, min}
/**
  * Region on chromosome
  * position is 0-based and the interval is half open half closed
  * [start, end)
  *
  * case classes:
  *   1. single: for snv
  *   2. interval: for cnv
  *   3. gene: for region with a name
  * */

@SerialVersionUID(7737730001L)
trait Region extends Serializable {
  def chr: Byte
  def start: Int
  def end: Int

  override def toString = s"$chr:$start-$end"
  def length = end - start

  def mid = start + length/2

  def overlap(that: ZeroLength): Boolean = false

  def overlap(that: Region): Boolean = {
    this.chr == that.chr && min(this.end, that.end) > max(this.start, that.start)
  }

  def intersect(that: Region): Region = {
    require(this overlap that, "this region must overlap that")
    Region(this.chr, max(this.start, that.start), min(this.end, that.end))
  }

  def in(that: Region): Boolean = {
    this.chr == that.chr && this.start >= that.start && this.end <= that.end
  }

  def ==(that: Region): Boolean = this.chr == that.chr && this.start == that.start && this.end == that.end

  def <(that: Region)(implicit ordering: Ordering[Region]): Boolean = {
    if (ordering.compare(this, that) < 0) true else false
  }

  def <=(that: Region)(implicit ordering: Ordering[Region]): Boolean = {
    if (this < that || this == that) true else false
  }
  def >(that: Region)(implicit ordering: Ordering[Region]): Boolean = {
    if (that < this) true else false
  }

  def >=(that: Region)(implicit ordering: Ordering[Region]): Boolean = {
    if (! (this < that)) true else false
  }
}

case class Single(chr: Byte, pos: Int) extends Region {
  val start = pos
  val end = pos + 1
  override def toString = s"$chr:$pos"
}
case class ZeroLength(chr: Byte, pos: Int) extends Region {
  val start = pos
  val end = pos
  override def overlap(that: Region) = false
}
case class Interval(chr: Byte, start: Int, end: Int) extends Region

case class Named(chr: Byte, start: Int, end: Int, name: String) extends Region

case class Variation(chr: Byte, pos: Int, ref: String, alt: String, var info: Option[String]) extends Region {
  def start = pos
  def end = pos + 1

  def this(region: Region, ref: String, alt: String, info: Option[String] = None) = {
    this(region.chr, region.start, ref, alt, info)
  }
  def mutType = Variant.mutType(ref, alt)
  override def toString = s"$chr:$pos-$end[$ref|$alt]${info match {case None => ""; case Some(i) => i}}"
  def toRegion: String = s"$chr:$pos-$end[$ref|$alt]"

  //def copy() = Variation(chr, start, end, ref, alt, info)

  def ==(that: Variation): Boolean = {
    this.asInstanceOf[Region] == that.asInstanceOf[Region] && this.ref == that.ref && this.alt == that.alt
  }
  def addInfo(k: String, v: String): Variation = {
    info match {
      case None => this.info = Some(s"$k=$v")
      case Some(i) => this.info = Some(s"$i;$k=$v")
    }
    this
  }
  def parseInfo: Map[String, String] = info match {
    case None => Map[String, String]()
    case Some(s) =>
      s.split(";").map{x =>
        val is = x.split("=")
        is(0) -> is(1)
      }.toMap
  }
}

object Variation {
  import Region.Chromosome
  def apply(x: String): Variation = {
    val p = """(?:chr)?([MTXY0-9]+):(\d+)-(\d+)\[([ATCG]+)\|([ATCG]+)\]""".r
    x match {
      case p(c, s, _, r, a) =>
        Variation(c.byte, s.toInt, r, a, None)
    }
  }
  implicit object VariationOrdering extends Ordering[Variation] {
    def compare(x: Variation, y: Variation): Int = {
      val c = Region.ord.compare(x, y)
      if (c == 0) {
        x.alt compare y.alt
      } else {
        c
      }
    }
  }
}

object Single {
  implicit object SingleOrdering extends Ordering[Single] {
    def compare(x: Single, y: Single): Int = {
      if (x.chr != y.chr) {
        x.chr compare y.chr
      } else {
        x.pos compare y.pos
      }
    }
  }
}


object Region {

  case object Empty extends Region {
    val chr = 0.toByte
    val start = 0
    val end = 0
  }

  implicit def ord[A <: Region] = new Ordering[A] {
    def compare(x: A, y: A): Int = {
      if (x.chr != y.chr) {
        x.chr compare y.chr
      } else if (x.start != y.start) {
        x.start compare y.start
      } else {
        x.end compare y.end
      }
    }
  }

  implicit class Chromosome(val self: String) extends AnyVal {
    def byte: Byte = {
      val num = """(\d+)""".r
      self match {
        case num(x) => x.toByte
        case "X" => 23.toByte
        case "Y" => 24.toByte
        case "M" => 25.toByte
        case "MT" => 25.toByte
        case "XY" => 26.toByte
        case _ => 0.toByte
      }
    }
  }

  def apply(c: Byte, p: Int): Region = Single(c, p)
  def apply(c: String, p: Int): Region = apply(c.byte, p)
  def apply(c: String, s: Int, e: Int): Region = {
    apply(c.byte, s, e)
  }
  def apply(c: Byte, s: Int, e: Int): Region = {
    require(e >= s, "end must be larger than or equal to start.")
    if (e == s)
      ZeroLength(c, s)
    else if (e - s > 1)
      Interval(c, s, e)
    else
      Single(c, s)
  }

  def apply(c: String, s: Int, e: Int, n: String): Region = Named(c.byte, s, e, n)

  def apply(pattern: String): Region = {
    val onlyChr = """(?i)(?:chr)?([MTXY0-9]+)""".r
    val start = """(?i)(?:chr)?([MTXY0-9]+):(\d+)-""".r
    val end = """(?i)(?:chr)?([MTXY0-9]+):-(\d+)""".r
    val full = """(?i)(?:chr)?([MTXY0-9]+):(\d+)-(\d+)""".r
    pattern match {
      case onlyChr(chr) => apply(chr, 0, Int.MaxValue)
      case start(chr, s) => apply(chr, s.toInt, Int.MaxValue)
      case end(chr, e) => apply(chr, 0, e.toInt)
      case full(chr, s, e) => apply(chr, s.toInt, e.toInt)
      case _ => Empty
    }
  }

}

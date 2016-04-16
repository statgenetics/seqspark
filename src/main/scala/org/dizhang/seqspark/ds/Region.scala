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

@SerialVersionUID(3L)
trait Region extends Serializable {
  val chr: Byte
  val start: Int
  val end: Int

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

case class Variation(chr: Byte, start: Int, end: Int, ref: String, alt: String) extends Region {
  def this(region: Region, ref: String, alt: String) = this(region.chr, region.start, region.end, ref, alt)
  def mutType = Variant.mutType(ref, alt)
  override def toString = s"$chr:$start-$end[$ref|$alt]"
  def ==(that: Variation): Boolean = {
    this.asInstanceOf[Region] == that.asInstanceOf[Region] && this.ref == that.ref && this.alt == that.alt
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
  /**
  implicit object RegionOrdering extends Ordering[Region] {
    def compare(x: Region, y: Region): Int = {
      if (x.chr != y.chr) {
        x.chr compare y.chr
      } else if (x.start != y.start) {
        x.start compare y.start
      } else {
        x.end compare y.end
      }
    }
  }
  */
  /**
    * implicit object StartOrdering extends Ordering[Region] {
    * def compare(a: Region, b: Region): Int = {
    * if (a.chr != b.chr)
    * a.chr compare b.chr
    * else
    * a.start compare b.start
    * }
    * }

    * implicit object MidOrdering extends Ordering[Region] {
    * def compare(a: Region, b: Region): Int = {
    * if (a.chr != b.chr)
    * a.chr compare b.chr
    * else
    * a.mid compare b.mid
    * }
    * }

    * implicit object EndOrdering extends Ordering[Region] {
    * def compare(a: Region, b: Region): Int = {
    * if (a.chr != b.chr)
    * a.chr compare b.chr
    * else
    * a.end compare b.end
    * }
    * }
    */
  implicit class Chromosome(val self: String) extends AnyVal {
    def byte: Byte = {
      val num = """(\d+)""".r
      self match {
        case num(x) => x.toByte
        case "X" => 23.toByte
        case "Y" => 24.toByte
        case "XY" => 25.toByte
        case "M" => 26.toByte
        case "MT" => 26.toByte
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
    val onlyChr = """(?:chr)?([MTXY0-9]+)""".r
    val start = """(?:chr)?([MTXY0-9]+):(\d+)-""".r
    val end = """(?:chr)?([MTXY0-9]+):-(\d+)""".r
    val full = """(?:chr)?([MTXY0-9]+):(\d+)-(\d+)""".r
    pattern match {
      case onlyChr(chr) => apply(chr, 0, Int.MaxValue)
      case start(chr, s) => apply(chr, s.toInt, Int.MaxValue)
      case end(chr, e) => apply(chr, 0, e.toInt)
      case full(chr, s, e) => apply(chr, s.toInt, e.toInt)
      case _ => apply("27", 0, 1)
    }
  }

/**
  * def apply[A](vars: Iterable[Variant[A]], n: Option[String] = None): Region = {
  * val r = vars.map(v => Region(v.chr, v.pos.toInt - 1, v.pos.toInt))
  * .reduce((a, b) => )
  * n match {
  * case None => r
  * case Some(s) =>
  * }
  * }
*/
  /**
    * def collapse(regs: List[Region]): List[Region] = {
    * }
    */
}

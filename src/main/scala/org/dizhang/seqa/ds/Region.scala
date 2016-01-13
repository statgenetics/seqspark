package org.dizhang.seqa.ds

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

  def length = end - start

  def mid = start + length/2

  def overlap(that: Region): Boolean = {
    this.chr == that.chr && min(this.end, that.end) > max(this.start, that.start)
  }
}

case class Single(chr: Byte, pos: Int) extends Region {
  val start = pos
  val end = pos + 1
}

case class Interval(chr: Byte, start: Int, end: Int) extends Region

case class Named(chr: Byte, start: Int, end: Int, name: String) extends Region

object Region {

  implicit object StartOrdering extends Ordering[Region] {
    def compare(a: Region, b: Region): Int = {
      if (a.chr != b.chr)
        a.chr compare b.chr
      else
        a.start compare b.start
    }
  }

  implicit object MidOrdering extends Ordering[Region] {
    def compare(a: Region, b: Region): Int = {
      if (a.chr != b.chr)
        a.chr compare b.chr
      else
        a.mid compare b.mid
    }
  }

  implicit object EndOrdering extends Ordering[Region] {
    def compare(a: Region, b: Region): Int = {
      if (a.chr != b.chr)
        a.chr compare b.chr
      else
        a.end compare b.end
    }
  }

  implicit class Chromosome(self: String) extends AnyVal {
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
  def apply(c: String, p: Int): Region = apply(c, p)
  def apply(c: String, s: Int, e: Int): Region = {
    apply(c.toByte, s, e)
  }
  def apply(c: Byte, s: Int, e: Int): Region = {
    require(e > s, "end must be larger than start.")
    if (e - s > 1)
      Interval(c, s, e)
    else
      Single(c, s)
  }

  def apply(c: String, s: Int, e: Int, n: String): Region = Named(c.byte, s, e, n)

  def apply(pattern: String): Region = {
    val onlyChr = """(?:chr)?(\d+)""".r
    val start = """(?:chr)?(\d+):(\d+)-""".r
    val end = """(?:chr)?(\d+):-(\d+)""".r
    val full = """(?:chr)?(\d+):(\d+)-(\d+)""".r
    pattern match {
      case onlyChr(chr) => apply(chr, 0, Int.MaxValue)
      case start(chr, s) => apply(chr, s.toInt, Int.MaxValue)
      case end(chr, e) => apply(chr, 0, e.toInt)
      case full(chr, s, e) => apply(chr, s.toInt, e.toInt)
      case _ => apply("27", 0, 0)
    }
  }

/**
  def apply[A](vars: Iterable[Variant[A]], n: Option[String] = None): Region = {
    val r = vars.map(v => Region(v.chr, v.pos.toInt - 1, v.pos.toInt))
      .reduce((a, b) => )
    n match {
      case None => r
      case Some(s) =>
    }
  }
*/
  /**
    * def collapse(regs: List[Region]): List[Region] = {
    * }
    */
}

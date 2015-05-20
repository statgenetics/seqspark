import scala.math.{min, max}

@SerialVersionUID(8L)
class Region(c: Byte, s: Int, e: Int) extends Serializable {
  val chr: Byte = c
  val start: Int = s 
  val end: Int = e

  def == (that: Region): Boolean = {
    if (this.chr == that.chr && this.start == that.start && this.end == that.end)
      true
    else
      false
  }

  def < (that: Region): Boolean = {
    if (this.chr < that.chr)
      true
    else if (this.chr == that.chr && this.end < that.start)
      true
    else
      false
  }

  def > (that: Region): Boolean =
    if (that < this) true else false

  def overlap (that: Region): Boolean = {
    if (this.chr == that.chr && max(this.start, that.start) < min(this.end, that.end))
      true
    else
      false
  }

  def cmp (that: Region): Int = {
    if (this < that)
      -1
    else if (this > that)
      1
    else
      0
  }

  def contains (c: Byte, p: Int): Boolean = {
    if (this.chr == c && p >= this.start && p <= this.end)
      true
    else
      false
  }

  def contains (cs: String, p: Int): Boolean = {
    val num = "(\\d+)".r
    val c = cs match {
      case num(x) => x.toByte
      case "X" => 23.toByte
      case "Y" => 24.toByte
      case "XY" => 25.toByte
      case "M" => 26.toByte
      case "MT" => 26.toByte
      case _ => 0.toByte
    }
    if (this.chr == c && p >= this.start && p <= this.end)
      true
    else
      false
  }


}

object Region {

  def apply(c: Byte, s: Int, e: Int): Region = {
    new Region(c, s, e)
  }

  def apply(cs: String, s: Int, e: Int): Region = {
    val num = "(\\d+)".r
    val c = cs match {
      case num(x) => x.toByte
      case "X" => 23.toByte
      case "Y" => 24.toByte
      case "XY" => 25.toByte
      case "M" => 26.toByte
      case "MT" => 26.toByte
      case _ => 0.toByte
    }
    apply(c, s, e)
  }
  /**
    * def collapse(regs: List[Region]): List[Region] = {
    * }
    */
}

import breeze.linalg.{Vector, DenseVector, VectorBuilder, SparseVector}
import breeze.math.Semiring
import scala.reflect.ClassTag
import Constants._

object Semi {

  /** To work with SparseVector[Byte], we need to extend breeze.math.Semiring to support Byte */
  @SerialVersionUID(1L)
  implicit object SemiByte extends Semiring[Byte] {
    def zero = 0.toByte

    def one = 1.toByte

    def +(a: Byte, b: Byte) = (a + b).toByte

    def *(a: Byte, b: Byte) = (a * b).toByte

    def ==(a: Byte, b: Byte) = a == b

    def !=(a: Byte, b: Byte) = a != b
  }

}

@SerialVersionUID(2L)
abstract class Variant[A] extends Serializable {
  import Semi.SemiByte
  val chr: String
  val pos: String
  val id: String
  val ref: String
  val alt: String
  val qual: String
  val filter: String
  val info: String
  val format: String
  val geno: Vector[A]
  val flip: Option[Boolean]

  override def toString = {
  import Variant._
    val gs: String = geno
    s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t$info\t$format\t$gs\n"
  }

  def meta(): Array[String] = {
    Array[String](chr, pos, id, ref, alt, qual, filter, info, format)
  }

  def site(): String = {
    s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t.\n"
  }

  def isTi: Boolean = {
    if (Set(ref, alt) == Set("A", "G") || Set(ref, alt) == Set("C","T"))
      true
    else
      false
  }

  def isTv: Boolean = {
    val cur = Set(ref, alt)
    if (cur == Set("A","C") || cur == Set("A","T") || cur == Set("G","C") || cur == Set("G","T"))
      true
    else
      false
  }

  def parseInfo: Map[String, String] = {
    if (this.info == ".")
      Map[String, String]()
    else
      (for {item <- this.info.split(";")
            s = item.split("=")}
        yield if (s.length == 1) (s(0) -> "true") else (s(0) -> s(1))).toMap
  }

  def parseFormat: Array[String] = {
    this.format.split(":")
  }

  def transElem[B](make: A => B)(implicit cm: ClassTag[B]): Variant[B] = {
    val newGeno = this.geno.map[B, Vector[B]](g => make(g))
    Variant[B](this.meta, newGeno, this.flip)
  }

  def transWhole[B](make: Vector[A] => Vector[B])(implicit cm: ClassTag[B]): Variant[B] = {
    val newGeno = make(this.geno)
    Variant[B](this.meta, newGeno, this.flip)
  }

  def compress(make: A => Byte): Variant[Byte] = {

    val denseGeno: Vector[Byte] = this.geno.map(g => make(g))

    val vb = new VectorBuilder[Byte](denseGeno.length)

    val newFlip =
      this.flip match {
        case Some(f) => Some(f)
        case None =>
          if (this.geno.findAll(a => a == 0).length > 0.5 * denseGeno.length)
            Some(true)
          else
            Some(false)
      }

    val newDenseGeno =
      if (newFlip.get)
        denseGeno.map(a => (2 - a).toByte)
      else
        denseGeno

    for (i <- 0 until newDenseGeno.length) {
      if (newDenseGeno(i) != 0)
        vb.add(i, newDenseGeno(i))
    }

    val newGeno: Vector[Byte] = vb.toSparseVector

    Variant[Byte](this.meta, newGeno, newFlip)
  }
}

object Variant {
  /** implicit convert Vector[Byte] to String */
  implicit def convertGenoByteToString(gb: Vector[Byte]): String = {
    gb.map(x => Bt.conv(x)).toArray.mkString("\t")
  }

  /** implicit convert Vector[String] to String */
  implicit def convertGenoStringToString(gb: Vector[String]): String = {
    gb.toArray.mkString("\t")
  }

  /** dummy other */
  implicit def convertGenoOtherToString[A](gb: Vector[A]): String = {
    gb.map(_.toString).toArray.mkString("\t")
  }

  /** Just store everything from the raw vcf file */
  def apply(line: String): Variant[String] = {
    val fields = line.split("\t")
    new Variant[String]() {
      val chr = fields(0)
      val pos = fields(1)
      val id = fields(2)
      val ref = fields(3)
      val alt = fields(4)
      val qual = fields(5)
      val filter = fields(6)
      val info = fields(7)
      val format = fields(8)
      val geno = new DenseVector[String](fields.slice(9, fields.length))
      val flip = None
    }
  }

  /** Some applies to initialize new instances */
  def apply[A](ma: Array[String], gv: Vector[A], fo: Option[Boolean]): Variant[A] = {
    new Variant[A] {
      val chr = ma(0)
      val pos = ma(1)
      val id = ma(2)
      val ref = ma(3)
      val alt = ma(4)
      val qual = ma(5)
      val filter = ma(6)
      val info = ma(7)
      val format = ma(8)
      val geno = gv
      val flip = fo
    }
  }

  import Constants.Bt

  def makeVCF(v: Variant[Byte]): String = {
    val meta = v.meta.mkString("\t")
    val fp = v.flip.getOrElse(false)
    val gs =
      if (fp)
        v.geno.map(g => Bt.conv(Bt.flip(g))).toArray.mkString("\t")
      else
        v.geno.map(g => Bt.conv(g)).toArray.mkString("\t")
    meta + "\t" + gs
  }

}


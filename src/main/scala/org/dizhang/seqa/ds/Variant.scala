package org.dizhang.seqa.ds

/**
 * a variant is a collection holding a row of VCF
 */

@SerialVersionUID(1L)
trait Variant[A] extends Serializable {
  def chr: String = meta(0)
  def pos: String = meta(1)
  def id: String = meta(2)
  def ref: String = meta(3)
  def alt: String = meta(4)
  def qual: String = meta(5)
  def filter: String = meta(6)
  def info: String = meta(7)
  def format: String = meta(8)
  def flip: Option[Boolean]
  def geno(implicit make: A => String): IndexedSeq[String]

  def apply(columnName: String): String = {
    columnName match {
      case "CHROM" => chr
      case "POS" => pos
      case "ID" => id
      case "REF" => ref
      case "ALT" => alt
      case "QUAL" => qual
      case "FILTER" => filter
      case "INFO" => info
      case "FLIP" => flip match {
        case Some(p) => if (p) "true" else "false"
        case None => "."
      }
      case _ => "."
    }
  }

  def toString(implicit make: A => String) = {
    s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t$info\t$format\t${geno.mkString('\t'.toString)}\n"
  }

  def meta: Array[String]

  def site: String = {
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
        yield if (s.length == 1) s(0) -> "true" else s(0) -> s(1)).toMap
  }

  def parseFormat: Array[String] = {
    this.format.split(":")
  }
}
/**
object Variant {

  private class StringVariant(val meta: Array[String],
                              val geno: Array[String],
                              val flip: Option[Boolean] = None) extends Variant[String] {
    def length = geno.length
    def apply(i: Int) = geno(i)
    override protected[this] def newBuilder: Builder[String, Variant[String]] =
      new ArrayBuffer mapResult fromSeqToStringVariant(meta, flip)
  }

  def fromSeqToStringVariant(ma: Array[String], fo: Option[Boolean])(gis: IndexedSeq[String]) =
    new StringVariant(ma, gis.toArray, fo)

  implicit def canBuildFrom: CanBuildFrom[StringVariant, ]

  private class ByteVariant(val meta: )

  /**
  def fromSeq[A](ma: Array[String], fo: Option[Boolean])(gis : IndexedSeq[A]): Variant[A] =
    new Variant[A] {
      private val elems = gis
      val meta = ma
      val flip = fo
      val length = elems.length
      def apply(i: Int) = elems(i)
      def geno = elems.map(x => x.toString).toArray
    }

  def newBuilder[A](ma: Array[String], fo: Option[Boolean]): Builder[A, Variant[A]] =


  implicit def canBuildFrom[A]: CanBuildFrom[Variant[_], A, Variant[A]] =
    new CanBuildFrom[Variant[_], A, Variant[A]] {
      def apply(): Builder[A, Variant[A]] = newBuilder(Array.fill(9)("."), None)
      def apply(from: Variant[_]): Builder[A, Variant[A]] =
        newBuilder(from.meta, from.flip)
    }


  def compress(v: Variant[String]): Variant[Byte] = {
    val elems =
      for {
        (x, i) <- v.zipWithIndex
        if (x != Gt.ref)
      } yield i -> x
    fromSeq[Byte](v.meta, v.flip)(v.geno)
  }

  /** Just store everything from the raw vcf file */
  def apply(line: String): Variant[String] = {
    val fields = line.split("\t")
    fromSeq(fields.slice(0,9), None)(fields.slice(9, fields.length))
  }

  /** Some applies to initialize new instances */
  def apply[A](ma: Array[String], ga: Array[A], fo: Option[Boolean]): Variant[A] = {
    fromSeq(ma, fo)(ga)
  }

  import Constant.Bt

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
  */
}
*/

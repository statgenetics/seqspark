@SerialVersionUID(3L)
abstract class Variant[A] extends Serializable {
  val chr: String
  val pos: Int
  val ref: String
  val alt: String
  val filter: String
  val info: Map[String, String]
  val format: List[String]
  val geno: Array[A]

  override def toString =
    s"$chr\t$pos\t${chr}-${pos}\t$ref\t$alt\t100\tPASS\tNO\t${format mkString (':'.toString)}\t${geno mkString ('\t'.toString)}"

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
}

object Variant {
  private class snv (v: Variant[String], gs: Array[String]) extends Variant[String] {
    val chr = v.chr
    val pos = v.pos
    val ref = v.ref
    val alt = v.alt
    val filter = v.filter
    val info = v.info
    val format = List("GT")
    val geno = gs
  }

  private class vcfVar (val line: String) extends Variant[String] {
    private val array = line.split("\t")
    val chr = array(0)
    val pos = array(1).toInt
    val ref = array(3)
    val alt = array(4)
    val filter = array(6)
    val info = {
      val res =
        for {item <- array(7).split(";")
          s = item.split("=")}
        yield if (s.length == 1) (s(0) -> "true") else (s(0) -> s(1))
      res.toMap
    }
    val format = array(8).split(":").toList
    val geno = array.slice(9,array.length)
  }

  def apply(line: String): Variant[String] = {
    new vcfVar(line)
  }
  def transElem(v: Variant[String], make: String => String): Variant[String] = {
    new snv(v, v.geno.map(g => make(g)))
  }
  def transWhole(v: Variant[String], make: Array[String] => Array[String]): Variant[String] = {
    new snv(v, make(v.geno))
  }
}

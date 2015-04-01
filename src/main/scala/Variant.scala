@SerialVersionUID(3L)
abstract class Variant[A] extends Serializable {
  val chr: String
  val pos: Int
  val ref: String
  val alt: String
  val filter: String
  val info: scala.collection.mutable.Map[String, String]
  val format: List[String]
  val geno: Array[A]
  //def toTitv(filterGeno: Boolean) : (Array[Int], Array[Int])

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
  private class vcfVar (val line: String) extends Variant[String] {
    private val array = line.split("\\t")
    val chr = array(0)
    val pos = array(1).toInt
    val ref = array(3)
    val alt = array(4)
    val filter = array(6)
    val info = {
      val res = scala.collection.mutable.Map[String, String]()
      for (item <- array(7).split(";")) {
        if (item.matches("=")) {
          val p = item.split("=")
          res += (p(0) -> p(1))
        } else {
          res += (item -> "true")
        }
      }
      res
    }
    val format = array(8).split(":").toList
    val geno = array.slice(9,array.length)
    /**
    def toTitv(filterGeno: Boolean): (Array[Int], Array[Int]) = {
      val cnt1: Array[Int] = for {
        gstring <- geno
        gfields = gstring.split(":")
        cnt = {
          if (gfields(0) == "./.")
            0
          else if (filterGeno == false || gfields(1) == "PASS")
            gfields(0).split("/").map(_.toInt).sum
          else
            0
        }} yield cnt
      val cnt2 = Array.fill(geno.length)(0)
      if (this.isTi)
        (cnt1, cnt2)
        //new Titv(cnt1, cnt2)
      else if (this.isTv)
        (cnt2, cnt1)
        //new Titv(cnt2, cnt1)
      else
        (cnt2, cnt2)
        //new Titv(cnt2, cnt2)
    }
      */
  }


  def apply(line: String): Variant[String] = {
    new vcfVar(line)
  }
}

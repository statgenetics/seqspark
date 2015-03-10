abstract class Variant {
  def chr: String
  def pos: Int
  def ref: String
  def alt: String
  def filter: String
  def info: scala.collection.mutable.Map[String, String]
  def format: List[String]
  def geno: Array[String]
  def toTitv(filterGeno: Boolean) : (Array[Int], Array[Int])
}

object Variant {
  private class vcfVar (val line: String) extends Variant {
    private val array = line.split("\\t")
    def chr = array(0)
    def pos = array(1).toInt
    def ref = array(3)
    def alt = array(4)
    def filter = array(6)
    def info = {
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
    def format = array(8).split(":").toList
    def geno = array.slice(9,array.length)
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
  }


  def apply(line: String): Variant = {
    new vcfVar(line)
  }
}

//import scala.reflect._
@SerialVersionUID(2L)
abstract class Gtinfo[T] extends Serializable {
  val elems: Array[T]
  def length = elems.length
}

/*
class 
  def add (that: Gtinfo[Gtfield[U]]) = {
    for (i <- Range(0, elems.length))
      this.elems(i) = this.elems(i) add that.elems(i)
    this
  }
}
 */
/*
object Gtinfo {

  apply

}
 */
/*
object Gtinfo {
  def apply(geno: Array[String])
    new Gtinfo {val elem = geno.map()}
  def apply(geno: Array[String], pass: String => Boolean, make: String => String): Gtinfo =
    new Gtinfo {val elem = geno.map(g => Gtfield(g, pass, make))}

  def apply(geno: Array[String], pass: String => Boolean, make: String => Int): Gtinfo =
    new Gtinfo {val elem = geno.map(g => Gtfield(g, pass, make))}

  def apply(geno: Array[String], pass: String => Boolean, make: String => (Double, Int)): Gtinfo =
    new Gtinfo {val elem = geno.map(g => Gtfield(g, pass, make))}

  def apply(geno: Array[String], pass: String => Boolean, make: String => (String, (Double, Int))): Gtinfo =
    new Gtinfo {val elem = geno.map(g => Gtfield(g, pass, make))}}
 */

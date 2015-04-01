@SerialVersionUID(1L)
abstract class Gtfield extends Serializable {
  type U
  val elem: U}

class Gt(e: String) extends Gtfield {
  type U = String
  val elem = e
  def + (that: Gt): Gt =
    new Gt(this.elem + that.elem)
  def headers = List("Gt")
  override def toString = elem}

class Cnt(e: Int) extends Gtfield {
  type U = Int
  val elem = e
  def + (that: Cnt): Cnt =
    new Cnt(this.elem + that.elem)
  def headers = List("Cnt")
  override def toString = elem.toString}

class Gq(e: (Double, Int)) extends Gtfield {
  type U = (Double, Int)
  val elem = e
  def total = elem._1
  def cnt = elem._2
  def mean = elem._1 / elem._2
  def + (that: Gq): Gq =
    new Gq((this.total + that.total, this.cnt + that.cnt))
  def headers = List("Gq1", "Gq2")
  override def toString = elem._1.toString + "\t" + elem._2.toString}

class Gqmap(e: scala.collection.mutable.Map[String, Gq]) extends Gtfield {
  type U = scala.collection.mutable.Map[String, Gq]
  val elem = e
  def headers = elem.keys.toList.map(x => List[String](".%s.total" format x, ".%s.cnt" format x)).reduce((a, b) => a ++ b)
  def + (that: Gqmap): Gqmap = {
    for (key <- elem.keys ++ that.elem.keys) {
      if (elem.contains(key) && that.elem.contains(key))
        elem(key) += that.elem(key)
      else if (that.elem.contains(key))
        elem(key) = that.elem(key)
    }
    this}
  override def toString = elem.values.toList.mkString("\t")}
/*
object Gtfield {

  def apply(gf: String, pass: String => Boolean, make: String => Any): Gtfield =
    make match {
      case x: (String => String) => {
        if (pass(gf))
          new Gt(make(gf))
        else
          new Gt("./.")
      }
      case x: (String => Int) => {
//  def apply(gf: String, pass: String => Boolean, make: String => Int): Gtfield =
        if (pass(gf))
          new Cnt(make(gf))
        else
          new Cnt(0)
      }
      case x: (String => (Double, Int)) => {
        //        def apply(gf: String, pass: String => Boolean, make: String => (Double, Int)): Gtfield =
        if (pass(gf))
          new Gq(make(gf))
        else
          new Gq((0, 0))
      }
      case x: (String => (String, (Double, Int))) => {
//  def apply(gf: String, pass: String => Boolean, make: String => (String, (Double, Int))): Gtfield =
        if (pass(gf)) {
          val res = make(gf)
          new Gqmap(scala.collection.mutable.Map[String, Gq](res._1 -> new Gq(res._2)))
        } else
          new Gqmap(scala.collection.mutable.Map[String, Gq]("./." -> new Gq((0, 0))))}
      }}
 */

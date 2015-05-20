import scala.reflect.ClassTag
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
//import it.unimi.dsi.fastutil.ints.Int2ObjectMap

@SerialVersionUID(4L)
trait SemiGroup[A] extends Serializable {
  /** define an associative operation */
  def op (x: A, y: A): A
}

trait Monoid[A] extends SemiGroup[A] {
  /** define a unit element */
  def e: A
}

trait Group[A] extends Monoid[A] {
  /** define an inverse operation */
  def inv (x: A): A
}

//class Count[B] extends Variant {

//}

object SemiGroup {
  type IMap = Int2IntOpenHashMap
  implicit object Atom extends SemiGroup[Double] {
    def op (x: Double, y: Double) = x + y
  }
  implicit object Pair extends SemiGroup[(Double, Double)] {
    def op (x: (Double, Double), y: (Double, Double)) = (x._1 + y._1, x._2 + y._2)
  }
  implicit object Triple extends SemiGroup[(Double, Double, Double)] {
    def op (x: (Double, Double, Double), y: (Double, Double, Double)) =
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
  }
  implicit object MapAtom extends SemiGroup[Map[String, Double]] {
    def op (x: Map[String, Double], y: Map[String, Double]) =
      x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0.0)))
  }
  implicit object MapPair extends SemiGroup[Map[String, (Double, Double)]] {
    def op (x: Map[String, (Double, Double)], y: Map[String, (Double, Double)]) =
      x ++ (
        for { (k, yv) <- y; xv = x.getOrElse(k, (0.0, 0.0)) }
        yield k -> ((yv._1 + xv._1, yv._2 + xv._2))
      )
  }
  implicit object MapCounter extends SemiGroup[Map[(Int, Int), Int]] {
    def op (x: Map[(Int, Int), Int], y: Map[(Int, Int), Int]) =
      x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0)))
  }
  implicit object MapI2I extends SemiGroup[IMap] {
    def op (x: IMap, y: IMap): IMap = {
      val res = new Int2IntOpenHashMap()
      res.putAll(x)
      val yi = y.keySet.iterator 
      while (yi.hasNext) {
        val key = yi.next
        res.addTo(key, y.get(key))
      }
      res
    }
  }
}

abstract class Count[A] extends Variant[A] {
  //override val geno: Array[A] = gs
  def collapse(implicit sg: SemiGroup[A]): A = geno reduce ((a, b) => sg.op(a, b))

  def collapseByBatch(batch: Array[Int])
                     (implicit sg: SemiGroup[A]): Map[Int, A] = {
    geno.zipWithIndex groupBy { case (c, i) => batch(i) } mapValues (
      c => c map (x => x._1) reduce ((a, b) => sg.op(a, b))
    ) map identity
  }

  def get = geno

  def add(that: Count[A])(implicit sg: SemiGroup[A]): Count[A] = {
    for (i <- 0 until geno.length)
      geno(i) = sg.op(geno(i), that.get(i))
    this
  }
}

object Count {
  def addByBatch[A](x: Map[Int, A], y: Map[Int, A])
                (implicit sg: SemiGroup[A]): Map[Int, A] = {
    for {
      (i, xa) <- x
      ya = y(i)
    } yield (i -> sg.op(xa, ya))
  }

  def addGeno[A: ClassTag](g1: Array[A], g2: Array[A])(implicit sg: SemiGroup[A]): Array[A] = {
    val res =
      for {
        i <- (0 until g1.length)
        e1 = g1(i)
        e2 = g2(i)
      } yield sg.op(e1, e2)
    res.toArray
  }

  def apply[A: ClassTag](v: Variant[String])(make: String => A): Count[A] =
    new Count[A]() {
      val chr = v.chr
      val pos = v.pos
      val ref = v.ref
      val alt = v.alt
      val filter = v.filter
      val info = v.info
      val format = v.format
      val geno: Array[A] = v.geno map (g => make(g))
    }
}

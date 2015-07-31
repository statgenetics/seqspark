import scala.reflect.ClassTag
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import breeze.collection.mutable.SparseArray

@SerialVersionUID(3L)
trait SemiGroup[A] extends Serializable {
  /** define an associative operation */
  def op (x: A, y: A): A
  /** I known I should have defined a Monoid here,
    * but I'm too lazy to define all the zeros.
    * The pow operation is valid as long as it only takes positive i */
  def pow (x: A, i: Int): A
}

object SemiGroup {
  type IMap = Int2IntOpenHashMap
  type PairInt = (Int, Int)
  type TripleInt = (Int, Int, Int)
  type PairDouble = (Double, Double)
  type TripleDouble = (Double, Double, Double)
  type MapCounter = Map[PairInt, Int]
  implicit object AtomInt extends SemiGroup[Int] {
    def op (x: Int, y: Int) = x + y
    def pow (x: Int, i: Int) = x * i
  }
  implicit object PairInt extends SemiGroup[PairInt] {
    def op (x: PairInt, y: PairInt) = (x._1 + y._1, x._2 + y._2)
    def pow(x: PairInt, i: Int) = (x._1 * i, x._2 * i)
  }
  implicit object TripleInt extends SemiGroup[TripleInt] {
    def op (x: TripleInt, y: TripleInt) =
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    def pow (x: TripleInt, i:Int) =
      (x._1 * i, x._2 * i, x._3 * i)
  }
  implicit object AtomDouble extends SemiGroup[Double] {
    def op (x: Double, y: Double) = x + y
    def pow(x: Double, i: Int) = x * i
  }
  implicit object PairDouble extends SemiGroup[PairDouble] {
    def op (x: PairDouble, y: PairDouble) = (x._1 + y._1, x._2 + y._2)
    def pow (x: PairDouble, i: Int) = (x._1 * i, x._2 * i)
  }
  implicit object TripleDouble extends SemiGroup[TripleDouble] {
    def op (x: TripleDouble, y: TripleDouble) =
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    def pow (x: TripleDouble, i:Int) =
      (x._1 * i, x._2 * i, x._3 * i)
  }

  implicit object MapCounter extends SemiGroup[MapCounter] {
    def op (x: MapCounter, y: MapCounter) =
      x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0)))
    def pow (x: MapCounter, i: Int) =
      for ((k,v) <- x) yield k -> (v * i)
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
    def pow (x: IMap, i: Int) = {
      val res = new SemiGroup.IMap()
      val xi = x.keySet.iterator
      while (xi.hasNext) {
        val key = xi.next
        res.addTo(key, x.get(key) * i)
      }
      res
    }
  }
  /*
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
    */
}

abstract class Count[A] {
  val cnt: SparseArray[A]

  def collapse(implicit sg: SemiGroup[A]): A = {
    cnt.compact()
    val elems = cnt.data.reduce((a, b) => sg.op(a, b))
    val zeros = sg.pow(cnt.default, cnt.size - cnt.activeSize)
    sg.op(elems, zeros)
  }

  def collapseByBatch(batch: Array[Int])
                     (implicit cm:ClassTag[A], sg: SemiGroup[A]): Map[Int, A] = {
    cnt.compact
    cnt.toArray.zipWithIndex groupBy { case (c, i) => batch(i) } mapValues (
      c => c map (x => x._1) reduce ((a, b) => sg.op(a, b))
    ) map identity
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

  def addGeno[A: ClassTag](g1: SparseArray[A], g2: SparseArray[A])(implicit sg: SemiGroup[A]): SparseArray[A] = {
    g1.compact()
    g2.compact()
    require(g1.default == g2.default && g1.size == g2.size)
    val res = new SparseArray[A](g1.size, g1.default)
    var offset = 0
    while( offset < g1.activeSize) {
      val index: Int = g1.indexAt(offset)
      val value: A = g1.valueAt(offset)
      res.update(index, value)
      offset += 1
    }
    offset = 0
    while( offset < g2.activeSize) {
      val index: Int = g2.indexAt(offset)
      val value: A = g2.valueAt(offset)
      if (g1.contains(index))
        res.update(index, sg.op(g1(index), value))
      else
        res.update(index, value)
    }
    res.compact()
    res
  }

  def apply[A, B: ClassTag](v: Variant[A], make: A => B, default: B)(implicit cm: ClassTag[A]): Count[B] = {
    val sCnt = v.geno.toArray.map(g => make(g)).zipWithIndex.filterNot(g => g._1 == default)
    val indexes: Array[Int] = sCnt.map (x => x._2)
    val values: Array[B] = sCnt.map (x => x._1)
    val newCnt = new SparseArray[B](indexes, values, indexes.length, v.geno.length, default)
    new Count[B]() {
      val chr = v.chr
      val pos = v.pos
      val id = v.id
      val ref = v.ref
      val alt = v.alt
      val qual = v.qual
      val filter = v.filter
      val info = v.info
      val format = v.format
      val cnt = newCnt
      val flip = v.flip
    }
  }
}

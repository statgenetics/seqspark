package org.dizhang.seqa.util
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
/**
 * Created by zhangdi on 8/18/15.
 */

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

@SerialVersionUID(3L)
trait SemiGroup[A] extends Serializable {
  /** define an associative operation */
  def op (x: A, y: A): A
  /** I known I should have defined a Monoid here,
    * but I'm too lazy to define all the zeros.
    * The pow operation is valid as long as it only takes positive i */
  def pow (x: A, i: Int): A
}
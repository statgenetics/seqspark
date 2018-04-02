package org.dizhang.seqspark.ds


object SemiGroup {
  type PairInt = (Int, Int)
  type TripleInt = (Int, Int, Int)
  type PairDouble = (Double, Double)
  type TripleDouble = (Double, Double, Double)
  type MapCounter = Map[PairInt, Int]
  type MapCounterLong = Map[Int, Long]

  implicit object Longs extends SemiGroup[Array[Long]] {
    val zero: Array[Long] = Array.emptyLongArray
    def op(x: Array[Long], y: Array[Long]): Array[Long] = {
      val res = if (x.length > y.length) x.clone() else y.clone()
      for (i <- 0 until math.min(x.length, y.length)) {
        res(i) = x(i) + y(i)
      }
      res
    }
    def pow(x: Array[Long], k: Int): Array[Long] = {
      val res = x.clone()
      for (i <- x.indices) {
        res(i) = x(i) * k
      }
      res
    }
  }

  implicit object Ints extends SemiGroup[Array[Int]] {

    val zero: Array[Int] = Array.emptyIntArray

    def op(x: Array[Int], y: Array[Int]): Array[Int] = {
      val res = if (x.length > y.length) x.clone() else y.clone()
      for (i <- 0 until math.min(x.length, y.length)) {
        res(i) = x(i) + y(i)
      }
      res
    }

    def pow(x: Array[Int], k: Int): Array[Int] = {
      val res = x.clone()
      for (i <- x.indices) {
        res(i) = x(i) * k
      }
      res
    }
  }

  implicit object AtomInt extends SemiGroup[Int] {
    def zero = 0
    def op (x: Int, y: Int) = x + y
    def pow (x: Int, i: Int) = x * i
  }
  implicit object PairInt extends SemiGroup[PairInt] {
    def zero = (0, 0)
    def op (x: PairInt, y: PairInt) = (x._1 + y._1, x._2 + y._2)
    def pow(x: PairInt, i: Int) = (x._1 * i, x._2 * i)
  }
  implicit object TripleInt extends SemiGroup[TripleInt] {
    def zero = (0, 0, 0)
    def op (x: TripleInt, y: TripleInt) =
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    def pow (x: TripleInt, i:Int) =
      (x._1 * i, x._2 * i, x._3 * i)
  }
  implicit object AtomDouble extends SemiGroup[Double] {
    def zero = 0
    def op (x: Double, y: Double) = x + y
    def pow(x: Double, i: Int) = x * i
  }
  implicit object PairDouble extends SemiGroup[PairDouble] {
    def zero = (0, 0)
    def op (x: PairDouble, y: PairDouble) = (x._1 + y._1, x._2 + y._2)
    def pow (x: PairDouble, i: Int) = (x._1 * i, x._2 * i)
  }
  implicit object TripleDouble extends SemiGroup[TripleDouble] {
    def zero = (0, 0, 0)
    def op (x: TripleDouble, y: TripleDouble) =
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    def pow (x: TripleDouble, i:Int) =
      (x._1 * i, x._2 * i, x._3 * i)
  }

  implicit object MapCounter extends SemiGroup[MapCounter] {
    def zero = Map[PairInt, Int]()
    def op (x: MapCounter, y: MapCounter) =
      x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0)))
    def pow (x: MapCounter, i: Int) =
      for ((k,v) <- x) yield k -> (v * i)
  }

  implicit object MapCounterLong extends SemiGroup[MapCounterLong] {
    def zero: MapCounterLong = Map.empty[Int, Long]
    def op (x: MapCounterLong, y: MapCounterLong): MapCounterLong = {
      x ++ (for ((k, v) <- y) yield k -> (v + x.getOrElse(k, 0L)))
    }
    def pow(x: MapCounterLong, n: Int): MapCounterLong = {
      for ((k,v) <- x) yield k -> (v * n)
    }
  }

}

@SerialVersionUID(7737260101L)
trait SemiGroup[A] extends Serializable {
  /** def zero here to help sparse operation
    * Note that zero is NOT necessarily the default of a sparse counter*/
  def zero: A
  /** define an associative operation */
  def op (x: A, y: A): A
  /** I known I should have defined a Monoid here,
    * but I'm too lazy to define all the zeros.
    * The pow operation is valid as long as it only takes positive i */
  def pow (x: A, i: Int): A
}
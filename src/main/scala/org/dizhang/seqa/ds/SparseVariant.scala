package org.dizhang.seqa.ds

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * The sparse implementation of the variant class
 * For sparse implementation, it is hard to fake an efficient subclass of indexedseq
 * So I just make some useful methods from scratch
 */

class SparseVariant[A](val meta: Array[String],
                       private val elems: Map[Int, A],
                       private val default: A,
                       val length: Int,
                       val flip: Option[Boolean])
  extends Variant[A] {

  def geno(implicit make: A => String): IndexedSeq[String] = {
    this.map(make(_)).toIndexedSeq
  }

  def apply(i: Int) = {
    require(i < length)
    elems.getOrElse(i, default)
  }

  def map[B](f: A => B): SparseVariant[B] = {
    val newDefault = f(default)
    val newElems: Map[Int, B] = elems.map{case (k, v) => k -> f(v)}
    new SparseVariant[B](this.meta, newElems, newDefault, length, this.flip)
  }

  def select(indicator: Array[Boolean]): SparseVariant[A] = {

    /** use recursive function to avoid map key test every time */
    val sortedKeys = elems.keys.toList.sorted
    @tailrec def labelsFunc(cur: Int, keys: List[Int], idx: Int, res: Map[Int, A]): (Map[Int, A], Int) = {
      if (keys == Nil) {
        val mid = ((cur + 1) until length) count (i => indicator(i))
        if (indicator(cur))
          (res + (idx -> apply (cur)), idx + 1 + mid)
        else
          (res, idx + mid)
      } else {
        var newIdx: Int = 0
        val next = keys.head
        val mid = ((cur + 1) until next) count (i => indicator(i))
        val newRes =
          if (indicator(cur)) {
            newIdx = idx + 1 + mid
            if (idx != 0 || apply(cur) != default)
              res + (idx -> apply(cur))
            else
              res
          } else {
            newIdx = idx + mid
            res
          }
        labelsFunc(next, keys.tail, newIdx, newRes)
      }
    }
    val res = labelsFunc(0, sortedKeys, 0, Map[Int, A]())

    new SparseVariant[A](meta, res._1, default, res._2, this.flip)
  }

  def toCounter[B](make: A => B) = {
    val tmp = this.map(make)
    Counter.fromMap(tmp.elems, tmp.default, length)
  }

  /**
  def update(i: Int, value: A): SparseVariant[A] = {
    val newElems = elems + (i -> value)
    new SparseVariant[A](this.meta, newElems, default, length, this.flip)
  }
  */
  def update(values: Map[Int, A]): SparseVariant[A] = {
    /** In this values Map, value could be default */
    val newElems = elems ++ values
    new SparseVariant[A](this.meta, newElems, default, length, this.flip)
  }

  def updateMeta(ma: Array[String]): SparseVariant[A] = {
    new SparseVariant[A](ma, elems, default, length, flip)
  }

  def compact(): SparseVariant[A] = {
    val newElems: Map[Int, A] = elems.filter{case (k, v) => v != default}
    new SparseVariant[A](this.meta, newElems, default, length, this.flip)
  }

  def toDenseVariant: DenseVariant[A] = {
    val denseElems = (0 until length).map(elems.getOrElse(_, default))
    new DenseVariant[A](meta, denseElems, flip)
  }

  def toIndexedSeq: IndexedSeq[A] = {
    for (i <- 0 until length) yield elems.getOrElse(i, default)
  }
}

package org.dizhang.seqa.ds

import scala.collection.IndexedSeqLike
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{ArrayBuffer, Builder}

/**
 * The dense implementation of the variant class
 */
class DenseVariant[A](val meta: Array[String],
                      private val elems: IndexedSeq[A],
                      val flip: Option[Boolean])
  extends Variant[A] with IndexedSeq[A] with IndexedSeqLike[A, DenseVariant[A]] {

  def geno(implicit make: A => String): IndexedSeq[String] = {
    elems.map(make(_))
  }

  def length = elems.length

  def apply(i: Int) = elems(i)

  def toCounter[B](make: A => B, default: A): Counter[B] = {
    val tmp = this.map(make)
    Counter.fromIndexedSeq(tmp.elems, make(default))
  }

  def updateMeta(ma: Array[String]): DenseVariant[A] = {
    new DenseVariant[A](ma, elems, flip)
  }

  override protected[this] def newBuilder: Builder[A, DenseVariant[A]] =
    DenseVariant.newBuilder(this.meta, this.flip)

  def toSparseVariant(default: A): SparseVariant[A] = {

    val newFlip =
      this.flip match {
        case Some(f) => Some(f)
        case None =>
          if (this.filter(_ == default).length > 0.5 * this.length)
            Some(true)
          else
            Some(false)
      }

    val sparseElems: Map[Int, A] =
      (for {
        i <- 0 until this.length
        if elems(i) != default
      } yield i -> elems(i)).toMap

    new SparseVariant[A](meta, sparseElems, default, length, newFlip)
  }
}

object DenseVariant {

  /** The DenseVariant[String] is so useful that we make an apply function for it */
  def apply(line: String): DenseVariant[String] = {
    val fields = line.split("\t")
    new DenseVariant[String](fields.slice(0,9), fields.slice(9, fields.length), None)
  }

  def fromSeq[A](ma: Array[String], fo: Option[Boolean])(gis: IndexedSeq[A]): DenseVariant[A] =
    new DenseVariant[A](ma, gis, fo)

  def newBuilder[A](ma: Array[String], fo: Option[Boolean]): Builder[A, DenseVariant[A]] =
    new ArrayBuffer mapResult fromSeq(ma, fo)

  implicit def canBuildFrom[A, B]: CanBuildFrom[DenseVariant[A], B, DenseVariant[B]] =
    new CanBuildFrom[DenseVariant[A], B, DenseVariant[B]] {
      def apply(): Builder[B, DenseVariant[B]] = newBuilder(Array.fill(9)("."), None)
      def apply(from: DenseVariant[A]): Builder[B, DenseVariant[B]] = newBuilder(from.meta, from.flip)
    }
}
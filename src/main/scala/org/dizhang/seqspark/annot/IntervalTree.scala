package org.dizhang.seqspark.annot

import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.ds.{Single, Region}
import IntervalTree._
import annotation.tailrec
/**
  * location tree to hold locations
  */
object IntervalTree {

  object Color extends Enumeration {
    type Color = Value
    val black = Value("black")
    val red = Value("red")
  }

  val B = Color.black
  val R = Color.red

  /** leftest position on genome */
  val leftest = Single(0, 0)

  def left[A <: Region](x: A): Single = Single(x.chr, x.start)
  def right[A <: Region](x: A): Single = Single(x.chr, x.end - 1)

  def blacken[A <: Region](it: IntervalTree[A]): IntervalTree[A] = {
    it match {
      case Leaf => it
      case Node(x, c, a, b, m) => Node(x, B, a, b, m)
    }
  }

  /**
    * The balance function is based on:
    *   Chris Okasaki. Red-Black Trees in a Functional Setting. J. Functional Programming 9(4) 471-477, July 1999
    *
    * It is augmented with a 'max' field for interval tree
    *
    * */

  def balance[A <: Region](it: IntervalTree[A]): IntervalTree[A] = {
    it match {
      case Node(z, B, Node(y, R, Node(x, R, a, b, m1), c, m2), d, m3) =>
        Node(y, R, Node(x, B, a, b, m1), Node(z, B, c, d, max(right(z), c.max, d.max)), m3)
      case Node(z, B, Node(x, R, a, Node(y, R, b, c, m2), m1), d, m3) =>
        Node(y, R, Node(x, B, a, b, max(right(x), a.max, b.max)), Node(z, B, c, d, max(right(z), c.max, d.max)), m3)
      case Node(x, B, a, Node(y, R, b, Node(z, R, c, d, m3), m2), m1) =>
        Node(y, R, Node(x, B, a, b, max(right(x), a.max, b.max)), Node(z, B, c, d, m3), m1)
      case Node(x, B, a, Node(z, R, Node(y, R, b, c, m2), d, m3), m1) =>
        Node(y, R, Node(x, B, a, b, max(right(x), a.max, b.max)), Node(z, B, c, d, max(right(z), c.max, d.max)), m1)
      case _ => it
    }
  }

  def insert[A <: Region](tree: IntervalTree[A], item: A)(implicit ordering: Ordering[A]): IntervalTree[A] = {
    def _insert(_it: IntervalTree[A], _x: A): IntervalTree[A] = {
      _it match {
        case Leaf() => Node(_x, R, Leaf(), Leaf(), right(_x))
        case Node(y, c, a, b, m) => ordering.compare(_x, y) match {
          case -1 => balance(Node(y, c, _insert(a, _x), b, max(m, right(_x))))
          case 0 => _it
          case _ => balance(Node(y, c, a, _insert(b, _x), max(m, right(_x))))
        }
      }
    }
    blacken(_insert(tree, item))
  }

  def lookup[A <: Region](tree: IntervalTree[A], item: Region)(implicit ordering: Ordering[A]): List[A] = {
    /**
    *def lookupSingle(trees: List[IntervalTree[A]], x: Single, accum: List[A]): List[A] = {
      *trees match {
        *case Nil => accum
        *case Leaf :: rs => lookupSingle(rs, x, accum)
        *case Node(d, _, l, r, m) :: rs =>
          *if (m < x) {
            *lookupSingle(rs, x, accum)
          *} else if (x < left(d)) {
            *lookupSingle(l :: rs, x, accum)
          *} else if (x <= right(d)) {
            *lookupSingle(l :: r :: rs, x, d :: accum)
          *} else {
            *lookupSingle(l :: r :: rs, x, accum)
          *}
      *}
    *}
      */
    @tailrec
    def lookupRegion(trees: List[IntervalTree[A]], x: Region, accum: List[A]): List[A] = {
      trees match {
        case Nil => accum
        case Leaf() :: rs => lookupRegion(rs, x, accum)
        case Node(d, _, l, r, m) :: rs =>
          if (m < left(x)) {
            lookupRegion(rs, x, accum)
          } else if (right(x) < left(d)) {
            lookupRegion(l :: rs, x, accum)
          } else if (left(x) <= right(d)) {
            lookupRegion(l :: r :: rs, x, d :: accum)
          } else {
            lookupRegion(l :: r :: rs, x, accum)
          }
      }
    }
    lookupRegion(List(tree), item, Nil)
  }
  def apply[A <: Region](iter: Iterator[A]): IntervalTree[A] = {
    def rec(i: Iterator[A], acc: IntervalTree[A]): IntervalTree[A] = {
      if (i.hasNext) {
        val cur = i.next()
        rec(i, insert(acc, cur))
      } else {
        acc
      }
    }
    rec(iter, Leaf())
  }
}

sealed trait IntervalTree[A <: Region] {
  def color: Color.Color
  def max: Single
}

case class Leaf[A <: Region]() extends IntervalTree[A] {
  val color = Color.black
  val max = leftest
}

case class Node[A <: Region](data: A,
                             color: Color.Color,
                             left: IntervalTree[A],
                             right: IntervalTree[A],
                             max: Single) extends IntervalTree[A]

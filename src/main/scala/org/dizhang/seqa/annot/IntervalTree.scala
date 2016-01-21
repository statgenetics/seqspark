package org.dizhang.seqa.annot

import breeze.linalg.max
import org.dizhang.seqa.ds.{Single, Region}
import IntervalTree._
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

  def rightBound[A <: Region](it: IntervalTree[A]): Single = {
    it match {
      case Leaf => leftest
      case Node(d, _, l, r) => max(Single(d.chr, d.end), rightBound(l), rightBound(r))
    }
  }

  def blacken[A <: Region](it: IntervalTree[A]): IntervalTree[A] = {
    it match {
      case Leaf => it
      case Node(x, c, a, b) => Node(x, B, a, b)
    }
  }

  def balance[A <: Region](it: IntervalTree[A]): IntervalTree[A] = {
    it match {
      case Node(z, B, Node(y, R, Node(x, R, a, b), c), d) =>
        Node(y, R, Node(x, B, a, b), Node(z, B, c, d))
      case Node(z, B, Node(x, R, a, Node(y, R, b, c)), d) =>
        Node(y, R, Node(x, B, a, b), Node(z, B, c, d))
      case Node(x, B, a, Node(y, R, b, Node(z, R, c, d))) =>
        Node(y, R, Node(x, B, a, b), Node(z, B, c, d))
      case Node(x, B, a, Node(z, R, Node(y, R, b, c), d)) =>
        Node(y, R, Node(x, B, a, b), Node(z, B, c, d))
      case Node(x, c, a, b) =>
        Node(x, B, a, b)
      case _ => it
    }
  }

  def insert[A <: Region](it: IntervalTree[A], x: A)(implicit ordering: Ordering[A]): IntervalTree[A] = {
    def _insert(_it: IntervalTree[A], _x: A): IntervalTree[A] = {
      _it match {
        case Leaf => Node(_x, R, Leaf, Leaf)
        case Node(y, c, a, b) => ordering.compare(_x, y) match {
          case -1 => balance(Node(y, c, _insert(a, _x), b))
          case 0 => _it
          case _ => balance(Node(y, c, a, _insert(b, _x)))
        }
      }
    }
    blacken(_insert(it, x))
  }
}

sealed trait IntervalTree[A <: Region] {
  def color: Color.Color
}

case object Leaf extends IntervalTree[Nothing] {
  val color = Color.black
}

case class Node[A <: Region](data: A,
                             color: Color.Color,
                             left: IntervalTree[A],
                             right: IntervalTree[A]) extends IntervalTree[A] {
  var max = rightBound(this)
}

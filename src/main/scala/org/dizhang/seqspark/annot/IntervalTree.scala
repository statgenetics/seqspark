/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.annot

import org.dizhang.seqspark.annot.IntervalTree._
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.General._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  * location tree to hold locations
  */
object IntervalTree {

  val logger = LoggerFactory.getLogger(this.getClass)

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
      case Leaf() => it
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
        Node(y, R, Node(x, B, a, b, m1), Node(z, B, c, d, max(right(z.head), c.max, d.max)), m3)
      case Node(z, B, Node(x, R, a, Node(y, R, b, c, m2), m1), d, m3) =>
        Node(y, R, Node(x, B, a, b, max(right(x.head), a.max, b.max)), Node(z, B, c, d, max(right(z.head), c.max, d.max)), m3)
      case Node(x, B, a, Node(y, R, b, Node(z, R, c, d, m3), m2), m1) =>
        Node(y, R, Node(x, B, a, b, max(right(x.head), a.max, b.max)), Node(z, B, c, d, m3), m1)
      case Node(x, B, a, Node(z, R, Node(y, R, b, c, m2), d, m3), m1) =>
        Node(y, R, Node(x, B, a, b, max(right(x.head), a.max, b.max)), Node(z, B, c, d, max(right(z.head), c.max, d.max)), m1)
      case _ => it
    }
  }

  def insert[A <: Region](tree: IntervalTree[A], item: A)(implicit ordering: Ordering[A]): IntervalTree[A] = {
    def _insert(_it: IntervalTree[A], _x: A): IntervalTree[A] = {
      _it match {
        case Leaf() => Node(List(_x), R, Leaf(), Leaf(), right(_x))
        case Node(y, c, a, b, m) => ordering.compare(_x, y.head) match {
          case n if n < 0 => balance(Node(y, c, _insert(a, _x), b, max(m, right(_x))))
          case 0 => if (y.contains(_x)) _it else Node(_x :: y, c, a, b, m)
          case _ => balance(Node(y, c, a, _insert(b, _x), max(m, right(_x))))
        }
      }
    }
    blacken(_insert(tree, item))
  }

  def lookup[A <: Region](tree: IntervalTree[A], item: Region)(implicit ordering: Ordering[A]): List[A] = {
    @tailrec
    def lookupRegion(trees: List[IntervalTree[A]], x: Region, accum: List[A]): List[A] = {
      trees match {
        case Nil => accum
        case Leaf() :: rs => lookupRegion(rs, x, accum)
        case Node(d, _, l, r, m) :: rs =>
          if (m < left(x)) {
            lookupRegion(rs, x, accum)
          } else if (right(x) < left(d.head)) {
            lookupRegion(l :: rs, x, accum)
          } else if (left(x) <= right(d.head)) {
            lookupRegion(l :: r :: rs, x, d ::: accum)
          } else {
            lookupRegion(l :: r :: rs, x, accum)
          }
      }
    }
    lookupRegion(List(tree), item, Nil)
  }

  def overlap[A <: Region](tree: IntervalTree[A], item: Region): Boolean = {
    lookup(tree, item) match {
      case Nil => false
      case _ => true
    }
  }

  def count[A <: Region](tree: IntervalTree[A]): Int = {
    @tailrec
    def countTree(trees: List[IntervalTree[A]], res: Int): Int = {
      trees match {
        case Nil => res
        case Leaf() :: rs => countTree(rs, res)
        case Node(d, _, l, r, _) :: rs =>
          countTree( l :: r :: rs, res + d.length)
      }
    }
    countTree(List(tree), 0)
  }

  def apply[A <: Region](iter: Iterator[A]): IntervalTree[A] = {
    def rec(i: Iterator[A], acc: IntervalTree[A]): IntervalTree[A] = {
      if (i.hasNext) {
        val cur = i.next()
        //logger.info(s"${cur.chr}:${cur.start}-${cur.end}")
        rec(i, insert(acc, cur))
      } else {
        acc
      }
    }
    rec(iter, Leaf[A]())
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

case class Node[A <: Region](data: List[A],
                             color: Color.Color,
                             left: IntervalTree[A],
                             right: IntervalTree[A],
                             max: Single) extends IntervalTree[A]

package org.dizhang.seqa.util

/**
  * some useful functions
  */
object General {

  def max[A](x: A, y: A)(implicit ordering: Ordering[A]): A = {
    ordering.compare(x, y) match {
      case -1 => y
      case _ => x
    }
  }

  def max[A](elems: A*)(implicit ordering: Ordering[A]): A = {
    elems.reduce((a, b) => max(a, b))
  }

}

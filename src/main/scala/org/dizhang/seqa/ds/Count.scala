package org.dizhang.seqa.ds

import org.dizhang.seqa.util.SemiGroup

import scala.reflect.ClassTag

/**
 * Created by zhangdi on 8/18/15.
 */
object Count {
  def addByBatch[A](x: Map[Int, A], y: Map[Int, A])
                (implicit sg: SemiGroup[A]): Map[Int, A] = {
    for {
      (i, xa) <- x
      ya = y(i)
    } yield (i -> sg.op(xa, ya))
  }

  def addGeno[A: ClassTag](g1: Array[A], g2: Array[A])(implicit sg: SemiGroup[A]): Array[A] = {
    for (i <- (0 until g1.length).toArray) yield sg.op(g1(i), g2(i))
  }

  def apply[A, B: ClassTag](v: Variant[A], make: A => B)(implicit cm: ClassTag[A]): Count[B] = {
    val newCnt = v.geno.toArray.map(make(_))
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

abstract class Count[A] {
  val cnt: Array[A]

  def collapse(implicit sg: SemiGroup[A]): A = {
    //cnt.compact()
    cnt.reduce((a, b) => sg.op(a, b))
  }

  def collapseByBatch(batch: Array[Int])
                     (implicit cm:ClassTag[A], sg: SemiGroup[A]): Map[Int, A] = {
    //cnt.compact
    cnt.zipWithIndex groupBy { case (c, i) => batch(i) } mapValues (
      c => c map (x => x._1) reduce ((a, b) => sg.op(a, b))
    ) map identity
  }
}
package org.dizhang.seqspark

/**
  * Created by zhangdi on 12/13/16.
  */
package object numerics {
  val s: Stream[Double] = 1.0 #:: s.map(f => f / 2.0)
  val EPSILON: Double = s.takeWhile(e => e + 1.0 != 1.0).last
  val MINVALUE: Double = Double.MinPositiveValue
  val MAXVALUE: Double = Double.MaxValue
}

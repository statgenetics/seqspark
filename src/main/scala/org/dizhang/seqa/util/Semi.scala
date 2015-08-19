package org.dizhang.seqa.util
import breeze.math.Semiring

/**
 * Created by zhangdi on 8/18/15.
 */
object Semi {

  /** To work with SparseVector[Byte], we need to extend breeze.math.Semiring to support Byte */
  @SerialVersionUID(1L)
  implicit object SemiByte extends Semiring[Byte] {
    def zero = 0.toByte

    def one = 1.toByte

    def +(a: Byte, b: Byte) = (a + b).toByte

    def *(a: Byte, b: Byte) = (a * b).toByte

    def ==(a: Byte, b: Byte) = a == b

    def !=(a: Byte, b: Byte) = a != b
  }

}

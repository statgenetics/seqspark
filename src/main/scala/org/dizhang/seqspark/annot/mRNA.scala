package org.dizhang.seqspark.annot

import org.dizhang.seqspark.util.Constant.Annotation.Nucleotide
import org.dizhang.seqspark.util.Constant.Annotation.Nucleotide.Nucleotide

/**
  * mRNA
  */
@SerialVersionUID(101L)
class mRNA(val name: String,
           val length: Int,
           seq: Array[Byte],
           na: Array[Int]) extends Serializable {
  def apply(i: Int): Nucleotide = {
    require(i < length)
    if (na.contains(i)) {
      Nucleotide.N
    } else {
      val idx = i/4
      val rem = i%4
      val seg = seq(idx)
      val tmp1 = seg << (rem * 2)
      val tmp2 = tmp1 >>> 6
      Nucleotide.values.toArray.apply(tmp2)
    }
  }
}

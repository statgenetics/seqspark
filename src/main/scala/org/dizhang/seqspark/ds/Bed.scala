package org.dizhang.seqspark.ds

import org.dizhang.seqspark.util.Constant

@SerialVersionUID(7737230001L)
class Bed (arg1: Array[Byte], arg2: Array[Byte]) extends Serializable {
  /**
    * This class is for holding the output buf,
    * we don't care about random access, just use two
    * Byte arrays for bim and bed
    */
  val bim = arg1
  val bed = arg2
}

object Bed {
  def apply(): Bed = {
    new Bed(Array[Byte](), Array[Byte]())
  }

  def apply(v: Variant[Byte]): Bed = {
    def makeBed (g: Byte): Byte = {
      /** not function right now */
      0

    }
    val id = "%s-%s" format(v.chr, v.pos)
    val bim: Array[Byte] =
      "%s\t%s\t%d\t%s\t%s\t%s\n"
        .format(v.chr,id,0,v.pos,v.ref,v.alt)
        .toArray
        .map(_.toByte)
    val bed: Array[Byte] =
      for {
        i <- Array[Int]() ++ (0 to v.length/4)
        four = 0 to 3 map (j => if (4 * i + j < v.length) makeBed(v(4 * i + j)) else 0.toByte)
      } yield
        four.zipWithIndex.map(a => a._1 << 2 * a._2).sum.toByte
    new Bed(bim, bed)
  }

  def add(a: Bed, b: Bed): Bed =
    new Bed(a.bim ++ b.bim, a.bed ++ b.bed)
}

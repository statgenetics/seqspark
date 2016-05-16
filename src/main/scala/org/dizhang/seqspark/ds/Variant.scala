package org.dizhang.seqspark.ds

import scala.annotation.tailrec
import org.dizhang.seqspark.util.UserConfig.MutType

import scala.reflect.ClassTag

/**
  * a variant is a collection holding a row of VCF
  * Note that VCF use 1-based coordinates
  * In our analysis, only bi-allelic sites are legal
  *
  * */


object Variant {
  val THRESHOLD = 0.25
  val MINIMIUM = 10000

  def fill[A](meta: Array[String], size: Int)(default: A): Variant[A] = {
    if (size == 0) {
      DummyVariant(meta, default)
    } else {
      SparseVariant(meta, Map.empty[Int, A], default, size)
    }
  }

  def fromString(line: String, default: String): Variant[String] = {
    val s = line.split("\t")
    fromIndexedSeq(s.slice(0, 9), s.slice(9, s.length), default)
  }

  def fromIndexedSeq[A](meta: Array[String], iseq: IndexedSeq[A], default: A): Variant[A] = {
    if (iseq.isEmpty)
      fill(meta, 0)(default)
    else {
      val denseSize = iseq.count { _ != default }
      val size = iseq.size
      if (size >= MINIMIUM && denseSize < size * THRESHOLD)
        SparseVariant(meta, toMap(iseq, default), default, size)
      else
        DenseVariant(meta, iseq, default, denseSize)
    }
  }
  def fromMap[A](meta: Array[String], m: Map[Int, A], default: A, size: Int): Variant[A] = {
    if (m.isEmpty)
      fill[A](meta, size)(default)
    else {
      val maxIdx = m.keys.max
      require(maxIdx < size, "max key (%s) exceeds valid for size (%s)" format(maxIdx, size))
      val denseSize = m.count( _._2 != default )
      if (size >= MINIMIUM && denseSize < (size * THRESHOLD))
        SparseVariant(meta, m, default, size)
      else
        DenseVariant(meta, toIndexedSeq(m, default, size), default, denseSize)
    }
  }

  def toIndexedSeq[A](m: Map[Int, A], default: A, size: Int): IndexedSeq[A] = {
    val buf = collection.mutable.Buffer.fill[A](size)(default)
    m.foreach {case (idx, v) => buf(idx) = v}
    IndexedSeq(buf: _*)
  }

  def toIndexedSeq[A](v: Variant[A]): IndexedSeq[A] =
    v match {
      case DenseVariant(_, iseq, _, _) => iseq
      case SparseVariant(_, m, default, size) => toIndexedSeq(m, default, size)
      case DummyVariant(_, _) => IndexedSeq[A]()
    }

  def toMap[A](iseq: IndexedSeq[A], default: A): Map[Int, A] =
    iseq.view.zipWithIndex.filter { _._1 != default }.map { _.swap }.toMap

  def toMap[A](v: Variant[A]): Map[Int, A] =
    v match {
      case DenseVariant(_, iseq, default, _) => toMap(iseq, default)
      case SparseVariant(_, m, _, _) => m
      case DummyVariant(_, _) => Map.empty[Int, A]
    }

  def toCounter[A, B](variant: Variant[A], make: A => B, default: B): Counter[B] = {
    variant match {
      case DenseVariant(_, iseq, _, _) => Counter.fromIndexedSeq(iseq.map(make), default)
      case SparseVariant(_, m, _, size) => Counter.fromMap(m.map(x => x._1 -> make(x._2)), default, size)
      case DummyVariant(_, _) => Counter.fill(0)(default)
    }
  }

  def mutType(ref: String, alt: String): MutType.Value = {
    val cnvReg = """[\[\]\<\>]""".r
    cnvReg.findFirstIn(alt) match {
      case Some(_) => MutType.cnv
      case None => if (ref.length > 1) {
        MutType.indel
      } else if (alt.split(",").forall(_.length == 1)) {
        MutType.snv
      } else {
        MutType.indel
      }
    }
  }

}

@SerialVersionUID(1L)
sealed trait Variant[A] extends Serializable {

  def size: Int
  def default: A
  def denseSize: Int
  def length: Int = size

  def chr: String = meta(0)
  def pos: String = meta(1)
  def id: String = meta(2)
  def ref: String = meta(3)
  def alt: String = meta(4)
  def qual: String = meta(5)
  def filter: String = meta(6)
  def info: String = meta(7)
  def format: String = meta(8)

  def alleles: Array[String] = Array(ref) ++ alt.split(",")

  def alleleNum: Int = alleles.length

  def mutType: MutType.Value = Variant.mutType(ref, alt)

  def geno(implicit make: A => String): IndexedSeq[String] =
    Variant.toIndexedSeq(this.map(g => make(g)))

  def map[B](f: A => B): Variant[B]

  def select(indicator: Array[Boolean]): Variant[A]

  def apply(i: Int): A

  def apply(columnName: String): String = {
    columnName match {
      case "CHROM" => chr
      case "POS" => pos
      case "ID" => id
      case "REF" => ref
      case "ALT" => alt
      case "QUAL" => qual
      case "FILTER" => filter
      case "INFO" => info
      case _ => "."
    }
  }

  def toCounter[B](make: A => B, d: B): Counter[B] = Variant.toCounter(this, make, d)

  def toString(implicit make: A => String) = {
    s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t$info\t$format\t${geno.mkString('\t'.toString)}"
  }

  var meta: Array[String]

  def site: String = {
    s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t$info"
  }

  def isTi: Boolean = {
    if (Set(ref, alt) == Set("A", "G") || Set(ref, alt) == Set("C","T"))
      true
    else
      false
  }

  def isTv: Boolean = {
    val cur = Set(ref, alt)
    if (cur == Set("A","C") || cur == Set("A","T") || cur == Set("G","C") || cur == Set("G","T"))
      true
    else
      false
  }

  def parseInfo: Map[String, String] = {
    if (this.info == ".")
      Map[String, String]()
    else
      (for {item <- this.info.split(";")
            s = item.split("=")}
        yield if (s.length == 1) s(0) -> "true" else s(0) -> s(1)).toMap
  }

  def addInfo(key: String, value: String): Unit = {
    require(! this.parseInfo.contains(key))
    this.meta(7) = s"$info;$key=$value"
  }

  def parseFormat: Array[String] = {
    this.format.split(":")
  }

  def updateMeta(ma: Array[String]): Variant[A] = {
    this.meta = ma
    this
  }

  def toIndexedSeq = Variant.toIndexedSeq(this)

  def toArray(implicit tag: ClassTag[A]) = this.toIndexedSeq.toArray

  def toRegion: Region = {
    Region(s"$chr:${pos.toInt - 1}-${pos.toInt + alleles.map(_.length).max - 1}")
  }

  def toVariation: Variation = new Variation(toRegion, ref, alt)

  def toDummy: DummyVariant[A] = {
    DummyVariant(meta, default)
  }
}

case class DummyVariant[A](var meta: Array[String], default: A)
  extends Variant[A] {
  def size = 0
  def select(indicator: Array[Boolean]): DummyVariant[A] = this
  def denseSize = 0
  def map[B](f: A => B): DummyVariant[B] = DummyVariant(meta, f(default))
  def apply(i: Int): A = default
}


/**
 * Dense Variant implementation
 * */

case class DenseVariant[A](var meta: Array[String],
                           elems: IndexedSeq[A],
                           default: A,
                           denseSize: Int)
  extends Variant[A] {

  def select(indicator: Array[Boolean]): Variant[A] = {
    val iseq =
      for {i <- elems.indices
        if indicator(i)} yield elems(i)
    Variant.fromIndexedSeq(meta, iseq, default)
  }

  def size = elems.length

  def apply(i: Int) = elems(i)

  def map[B](f: A => B): Variant[B] = {
    val iseq = elems.map(f)
    Variant.fromIndexedSeq(meta, iseq, f(default))
  }

}

/**
 * Sparse Variant
 * */

case class SparseVariant[A](var meta: Array[String],
                       elems: Map[Int, A],
                       default: A,
                       size: Int)
  extends Variant[A] {

  def denseSize = elems.size

  def apply(i: Int) = {
    require(i < length)
    elems.getOrElse(i, default)
  }

  def map[B](f: A => B): Variant[B] = {
    val newDefault = f(default)
    val newElems: Map[Int, B] = elems.map{case (k, v) => k -> f(v)}
    Variant.fromMap(meta, newElems, newDefault, size)
  }

  def select(indicator: Array[Boolean]): Variant[A] = {

    /** use recursive function to avoid map key test every time */
    val sortedKeys = elems.keys.toList.sorted
    @tailrec def labelsFunc(cur: Int, keys: List[Int], idx: Int, res: Map[Int, A]): (Map[Int, A], Int) = {
      if (keys == Nil) {
        val mid = ((cur + 1) until length) count (i => indicator(i))
        if (indicator(cur))
          (res + (idx -> apply (cur)), idx + 1 + mid)
        else
          (res, idx + mid)
      } else {
        var newIdx: Int = 0
        val next = keys.head
        val mid = ((cur + 1) until next) count (i => indicator(i))
        val newRes =
          if (indicator(cur)) {
            newIdx = idx + 1 + mid
            if (idx != 0 || apply(cur) != default)
              res + (idx -> apply(cur))
            else
              res
          } else {
            newIdx = idx + mid
            res
          }
        labelsFunc(next, keys.tail, newIdx, newRes)
      }
    }
    val res = labelsFunc(0, sortedKeys, 0, Map[Int, A]())
    Variant.fromMap(meta, res._1, default, res._2)
  }
}

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

package org.dizhang.seqspark.ds

import org.dizhang.seqspark.util.ConfigValue.MutType

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * a variant is a collection holding a row of VCF
  * Note that VCF use 1-based coordinates
  * In our analysis, only bi-allelic sites are legal
  *
  * */


object Variant {
  val THRESHOLD = 0.25
  val MINIMIUM = 1000

  /**
    * Define info and meta class
    *
    * */
  case class Info(original: String, addon: Map[String, String])


  def fill[A: Genotype](meta: Array[String], size: Int)(default: A): Variant[A] = {
    if (size == 0) {
      DummyVariant(meta, default)
    } else {
      SparseVariant(meta, Map.empty[Int, A], default, size)
    }
  }
  def fromImpute2(line: String, default: (Double, Double, Double)): Variant[(Double, Double, Double)] = {
    val s = line.split("\\s")
    val meta = Array(s(0), s(2), s(1), s(3), s(4), ".", ".", ".")
    val geno = s.slice(5, s.length).map(_.toDouble)
    val iseq = for (i <- 0 until geno.length/3)
      yield (geno(i), geno(i+1), geno(i+2))
    fromIndexedSeq(meta, iseq, default)
  }

  def fromString(line: String, default: String, noSample: Boolean = false): Variant[String] = {
    val s = line.split("\t")
    if (s.length == 8) {
      fill(s, 0)(default)
    } else if (noSample) {
      fill(s.slice(0,9), 0)(default)
    } else {
      fromIndexedSeq(s.slice(0, 9), s.slice(9, s.length), default)
    }
  }

  def fromIndexedSeq[A: Genotype](meta: Array[String], iseq: IndexedSeq[A], default: A): Variant[A] = {
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
  def fromMap[A: Genotype](meta: Array[String], m: Map[Int, A], default: A, size: Int): Variant[A] = {
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

  def toIndexedSeq[A: Genotype](m: Map[Int, A], default: A, size: Int): IndexedSeq[A] = {
    val buf = collection.mutable.Buffer.fill[A](size)(default)
    m.foreach {case (idx, v) => buf(idx) = v}
    IndexedSeq(buf: _*)
  }

  def toIndexedSeq[A: Genotype](v: Variant[A]): IndexedSeq[A] =
    v match {
      case DenseVariant(_, iseq, _, _) => iseq
      case SparseVariant(_, m, default, size) => toIndexedSeq(m, default, size)
      case DummyVariant(_, _) => IndexedSeq[A]()
    }

  def toMap[A: Genotype](iseq: IndexedSeq[A], default: A): Map[Int, A] =
    iseq.view.zipWithIndex.filter { _._1 != default }.map { _.swap }.toMap

  def toMap[A: Genotype](v: Variant[A]): Map[Int, A] =
    v match {
      case DenseVariant(_, iseq, default, _) => toMap(iseq, default)
      case SparseVariant(_, m, _, _) => m
      case DummyVariant(_, _) => Map.empty[Int, A]
    }

  def toCounter[A: Genotype, B](variant: Variant[A], make: A => B, default: B): Counter[B] = {
    variant match {
      case DenseVariant(_, iseq, _, _) => Counter.fromIndexedSeq(iseq.map(make), default)
      case SparseVariant(_, m, _, size) => Counter.fromMap(m.map(x => x._1 -> make(x._2)), default, size)
      case DummyVariant(_, _) => Counter.fill(0)(default)
    }
  }

  def parseInfo(info: String): Map[String, String] = {
    if (info == ".")
      Map[String, String]()
    else
      (for {
        item <- info.split(";")
        s = item.split("=")
        if item != "."
      } yield if (s.length == 1) s(0) -> "true" else s(0) -> s(1)).toMap
  }

  def serializeInfo(im: Map[String, String]): String = {
    if (im.isEmpty)
      "."
    else
      im.map{
        case (k, v) =>
          if (v == "true") k else s"$k=$v"
      }.mkString(";")
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

@SerialVersionUID(7737820001L)
abstract class Variant[A: Genotype] extends Serializable {

  def size: Int
  def default: A
  def denseSize: Int
  def length: Int = size

  private def gt = implicitly[Genotype[A]]
  def chr: String = meta(0)
  def pos: String = meta(1)
  def id: String = meta(2)
  def ref: String = meta(3)
  def alt: String = meta(4)
  def qual: String = meta(5)
  def filter: String = meta(6)
  def info: String = meta(7)
  def format: String = if (meta.length == 9) meta(8) else ""

  def alleles: Array[String] = Array(ref) ++ alt.split(",")

  def alleleNum: Int = alleles.length

  def mutType: MutType.Value = Variant.mutType(ref, alt)

  def geno: IndexedSeq[String] =
    Variant.toIndexedSeq(this.map(g => gt.toVCF(g)))

  def map[B: Genotype](f: A => B): Variant[B]

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

  override def toString = {
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
    Variant.parseInfo(info)
  }

  def addInfo(key: String): Unit = {
    if (! this.parseInfo.contains(key)) {
      this.meta(7) = s"$info;$key"
    }
  }

  def addInfo(key: String, value: String): Unit = {
    //require(! this.parseInfo.contains(key))

    if (this.parseInfo.contains(key)) {
      println(s"WARN: info key exists -- key: $key value: ${parseInfo(key)} new value: $value")
    }

    this.meta(7) = s"$info;$key=$value"
  }

  def updateInfo(im: Map[String, String]): Unit = {
    this.meta(7) = Variant.serializeInfo(parseInfo ++ im)
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

  def toVariation(withInfo: Boolean = false): Variation =
    new Variation(toRegion, ref, alt, if (withInfo) Some(info) else None)

  def toDummy: DummyVariant[A] = {
    DummyVariant(meta, default)
  }

  def copy: Variant[A] = {
    this match {
      case SparseVariant(m, e, d, s) =>
        SparseVariant(Array[String]() ++ m, Map[Int, A]() ++ e, d, s)
      case DenseVariant(m, e, d, s) =>
        DenseVariant(Array[String]() ++ m, IndexedSeq[A]() ++ e, d, s)
      case _ =>
        DummyVariant(Array[String]() ++ meta, default)
    }
  }
}

@SerialVersionUID(7737820101L)
case class DummyVariant[A: Genotype](var meta: Array[String], default: A)
  extends Variant[A] {
  def size = 0
  def select(indicator: Array[Boolean]): DummyVariant[A] = this
  def denseSize = 0
  def map[B: Genotype](f: A => B): DummyVariant[B] = DummyVariant(meta.clone(), f(default))
  def apply(i: Int): A = default

  override def toString: String = s"$chr\t$pos\t$id\t$ref\t$alt\t$qual\t$filter\t$info"
}


/**
 * Dense Variant implementation
 * */

@SerialVersionUID(7737820201L)
case class DenseVariant[A: Genotype](var meta: Array[String],
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

  def map[B: Genotype](f: A => B): Variant[B] = {
    val iseq = elems.map(f)
    Variant.fromIndexedSeq(meta.clone(), iseq, f(default))
  }

}

/**
 * Sparse Variant
 * */

@SerialVersionUID(7737820301L)
case class SparseVariant[A: Genotype](var meta: Array[String],
                       elems: Map[Int, A],
                       default: A,
                       size: Int)
  extends Variant[A] {

  def denseSize = elems.size

  def apply(i: Int) = {
    require(i < length)
    elems.getOrElse(i, default)
  }

  def map[B: Genotype](f: A => B): Variant[B] = {
    val newDefault = f(default)
    val newElems: Map[Int, B] = elems.map{case (k, v) => k -> f(v)}.filter(p => p._2 != newDefault)
    Variant.fromMap(meta.clone(), newElems, newDefault, size)
  }

  def select(indicator: Array[Boolean]): Variant[A] = {

    val oldMap = elems.toIndexedSeq.sortBy(_._1)
    if (oldMap.isEmpty) {
      /** if dense empty, just change the size */
      Variant.fromMap(meta, elems, default, indicator.count(_ == true))
    } else {
      /** new index initial value */
      var newIdx = (0 until oldMap.head._1).count(i => indicator(i))
      /** use map builder to hold the result */
      val builder = new mutable.MapBuilder[Int, A, Map[Int, A]](Map[Int, A]())
      for (i <- oldMap.indices) {
        /** include the non-default value if it's in the indicator */
        if (indicator(oldMap(i)._1))
          builder.+=(newIdx -> oldMap(i)._2)
        /** re-compute the new index by adding the number of defaults between two dense values (or the end) */
        if (i == oldMap.length - 1) {
          newIdx += (oldMap(i)._1 until length).count(i => indicator(i))
        } else {
          newIdx += (oldMap(i)._1 until oldMap(i+1)._1).count(i => indicator(i))
        }
      }
      val newMap = builder.result()
      Variant.fromMap(meta, newMap, default, newIdx)
    }
    /**
    /** this implementation is problematic */
    /** use recursive function to avoid map key test every time */
    val sortedKeys = elems.keys.toList.sorted
    @tailrec def labelsFunc(cur: Int, keys: List[Int], idx: Int, res: Map[Int, A]): (Map[Int, A], Int) = {
      if (keys == Nil) {
        val mid = (cur until length) count (i => indicator(i))
        if (indicator(cur))
          (res + (idx -> apply (cur)), idx + mid)
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
    */
  }
}

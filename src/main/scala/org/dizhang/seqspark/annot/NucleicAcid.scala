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

import org.dizhang.seqspark.annot.NucleicAcid._
import org.dizhang.seqspark.util.Constant.Annotation.Base.Base
import org.dizhang.seqspark.util.Constant.Annotation.{AminoAcid, Base, codeTable}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.{IndexedSeqLike, mutable}

/**
  * DNA, cDNA/mRNA sequences
  */
object NucleicAcid {
  private val S = 2
  private val N = 32/S
  private val M = (1 << S) - 1
  private val BaseArray = Base.values.toArray
  private val BaseMap = BaseArray.zipWithIndex.toMap
  private val CharBase = Map[Char, Base]('T' -> Base.T, 'C' -> Base.C, 'A' -> Base.A, 'G' -> Base.G, 'N' ->Base.N)
  private val BaseChar = CharBase.map(_.swap)
  def baseFromInt(i: Int): Base = BaseArray(i)
  def baseToInt(base: Base): Int = BaseMap(base)
  def baseFromChar(c: Char): Base = CharBase(c)
  def baseToChar(base: Base): Char = BaseChar(base)

  implicit class RichBase(val base: Base.Base) extends AnyVal {
    def toInt: Int = baseToInt(base)
    def toChar: Char = baseToChar(base)
  }

  @SerialVersionUID(7726620101L)
  case class DNA(groups: Array[Int], length: Int, na: Set[Int]) extends NucleicAcid

  @SerialVersionUID(7726620201L)
  case class Codon(group: Int) extends NucleicAcid {
    val length = 3
    val na = Set.empty[Int]
    val groups = Array(group)
    def update(i: Int, alt: Base): Codon = {
      Codon(~(M << (i * 2)) & group | (baseToInt(alt) << (i * 2)))
    }
    def translate: AminoAcid.AminoAcid = {
      val idx = this.zipWithIndex
        .map{case (b, i) => b.toInt * math.pow(4, 2 - i).toInt}.sum
      codeTable(idx)
    }
  }

  @SerialVersionUID(7726620301L)
  case class mRNA(groups: Array[Int],
                  length: Int,
                  na: Set[Int],
                  name: String) extends NucleicAcid {
    def getCodon(idx: Int, cds: (Int, Int), alt: Option[Base] = None): Codon = {
      require( idx >= cds._1 && idx < cds._2 , "index must in CDS")
      val phase = (idx - cds._1)%3
      val codon = phase match {
        case 0 => Codon(this.slice(idx, idx + 3).groups(0))
        case 1 => Codon(this.slice(idx - 1, idx + 2).groups(0))
        case _ => Codon(this.slice(idx - 2, idx + 1).groups(0))
      }
      if (alt.isDefined)
        codon.update(phase, alt.get)
      else
        codon
    }
  }

  private def fromSeq(buf: Seq[Base]): NucleicAcid = {
    val groups = new Array[Int]((buf.length + N - 1)/N)
    for (i <- buf.indices)
      groups(i/N) |= baseToInt(buf(i)) << (i % N * S)
    val na =
      (for {
        i <- buf.indices
        if buf(i) == Base.N
      } yield i).toSet
    if (buf.length == 3 && na == Set.empty[Int])
      Codon(groups(0))
    else
      DNA(groups, buf.length, na)
  }

  private def fromString(buf: String): NucleicAcid = {
    fromSeq(buf.map(c => baseFromChar(c.toUpper)))
  }

  def apply(raw: String): NucleicAcid = fromString(raw)

  def makeRNA(name: String, raw: String): mRNA = {
    val dna = fromString(raw)
    mRNA(dna.groups, dna.length, dna.na, name)
  }

  private def newBuilder: mutable.Builder[Base, NucleicAcid] = {
    new ArrayBuffer[Base] mapResult fromSeq
  }

  implicit def canBuildFrom: CanBuildFrom[NucleicAcid, Base, NucleicAcid] = {
    new CanBuildFrom[NucleicAcid, Base, NucleicAcid] {
      def apply(): mutable.Builder[Base, NucleicAcid] = newBuilder
      def apply(from: NucleicAcid): mutable.Builder[Base, NucleicAcid] = newBuilder
    }
  }
}

@SerialVersionUID(7726620001L)
sealed trait NucleicAcid
extends IndexedSeq[Base] with IndexedSeqLike[Base, NucleicAcid] with Serializable {
  val groups: Array[Int]
  val na: Set[Int]
  def apply(i: Int): Base = {
    require(i >=0 && i < length, "index out of range")
    if (na.contains(i))
      Base.N
    else
      baseFromInt(groups(i/N) >> (i % N * S) & M)
  }
  override protected[this] def newBuilder: mutable.Builder[Base, NucleicAcid] = {
    NucleicAcid.newBuilder
  }
  override def foreach[B](f: Base => B): Unit = {
    var i = 0
    var b = 0
    while (i < length) {
      b = if (i%N == 0) groups(i/N) else b >>> S
      f(baseFromInt(b & M))
      i += 1
    }
  }
  override def toString: String = {
    this.map(baseToChar(_)).mkString
  }

}


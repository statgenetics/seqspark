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
import org.apache.spark.broadcast.Broadcast
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.Constant

import scala.language.implicitConversions
/**
  * Created by zhangdi on 8/16/16.
  */
@SerialVersionUID(1L)
class VariantAnnotOp[A](val v: Variant[A]) extends Serializable {
  def annotateByVariant(dict: Broadcast[RefGene]): Variant[A] = {

    if (! v.alt.matches("""[ATCG]+""")) {
      v.addInfo(IK.anno, F.Unknown.toString)
      v
    } else {
      val variation = v.toVariation()

      val annot = IntervalTree.lookup(dict.value.loci, variation).filter(l =>
        dict.value.seq.contains(l.mRNAName)).map{l =>
        (l.geneName, l.mRNAName, l.annotate(variation, dict.value.seq(l.mRNAName)))}
      annot match {
        case Nil =>
          //logger.warn(s"no annotation for variant ${variation.toString}")
          v.addInfo(IK.anno, F.InterGenic.toString)
          v
        case _ =>
          //val consensus = annot.map(p => (p._1, p._3))
          //  .reduce((a, b) => if (FM(a._2) < FM(b._2)) a else b)
          val merge = annot.map(p => (p._1, s"${p._2}:${p._3.toString}")).groupBy(_._1)
            .map{case (k, value) => "%s::%s".format(k, value.map(x => x._2).mkString(","))}
            .mkString(",,")
          v.addInfo(IK.anno, merge)
          v
      }
    }
  }

  def groupByGene(onlyFunctional: Boolean = true): Array[(String, Variant[A])] = {
    /** the argument RefGene is the whole set of all genes involved
      * the output RefGene each represents a single gene
      * */

    val variation = v.toVariation()

    val anno = v.parseInfo(IK.anno)
    val genes =
      if (FM(worstAnnotation(anno)) >= FM(F.InterGenic))
        Array[(String, F.Value)]()
      else
        parseAnnotation(v.parseInfo(IK.anno))

    val res = if (onlyFunctional) {
      genes.filter(p => FM(p._2) <= 4)
    } else {
      genes
    }
    res.map{
      case (g, _) => g -> v.copy
    }
  }

  def groupByRegion(windowSize: Int, overlapSize: Int): Array[(String, Variant[A])] = {
    require(overlapSize < 0.5 * windowSize, "overlap must be less than half of the window size")
    val variantRegion = v.toRegion
    val unitSize = windowSize - overlapSize
    val unit = variantRegion.start / unitSize
    val unitPos = variantRegion.start % unitSize
    val startRegion = Region(v.chr, unit * unitSize, unit * unitSize + windowSize)
    val endRegion =
      if (variantRegion overlap Region(v.chr, (unit + 1) * unitSize)) {
        Region(v.chr, (unit + 1) * unitSize, (unit + 1) * unitSize + windowSize)
      } else {
        startRegion
      }
    if ((unit == 0 || unitPos > overlapSize) && startRegion == endRegion) {
      //v.addInfo("Region", startRegion.toString)
      Array(startRegion.toString -> v)
    } else {
      val v2 = v.copy
      //v.addInfo("Region", startRegion.toString)
      //v2.addInfo("Region", endRegion.toString)
      Array(startRegion.toString -> v, endRegion.toString -> v2)
    }
  }
}

object VariantAnnotOp {

  val F = Constant.Annotation.Feature
  val FM = F.values.zipWithIndex.toMap
  val Nucleotide = Constant.Annotation.Base
  val IK = Constant.Variant.InfoKey
  type Genes = Map[String, List[Location]]
  implicit def addAnnotOp[A](v: Variant[A]): VariantAnnotOp[A] = {
    new VariantAnnotOp[A](v)
  }
  def worstAnnotation(value: String): F.Value = {
    if (value == F.InterGenic.toString) {
      F.InterGenic
    } else if (value == F.CNV.toString) {
      F.CNV
    } else if (value == F.Unknown.toString) {
      F.Unknown
    } else {
      val genes = parseAnnotation(value)
      genes.map(_._2).reduce((a, b) => if (FM(a) < FM(b)) a else b)
    }
  }
  def parseAnnotation(value: String): Array[(String, F.Value)] = {
    val geneRegex = """([\w][\w-\.]*)::(\S+)""".r
    val trsRegex = """(\w+):(\S+)""".r
    val genes = value.split(",,").map{
      case geneRegex(gene, rest) => gene -> rest.split(",").map{
        case trsRegex(_, func) => F.withName(func)
      }.reduce((a, b) => if (FM(a) < FM(b)) a else b)
    }
    /** an array of genes, and their most deleterious function annotation */
    genes
  }
}

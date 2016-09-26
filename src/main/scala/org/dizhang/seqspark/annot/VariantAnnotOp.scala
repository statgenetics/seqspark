package org.dizhang.seqspark.annot
import org.apache.spark.broadcast.Broadcast
import org.dizhang.seqspark.annot.{IntervalTree, Location, RefGene, dbNSFP}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.Constant
import VariantAnnotOp._

/**
  * Created by zhangdi on 8/16/16.
  */
@SerialVersionUID(1L)
class VariantAnnotOp[A](val v: Variant[A]) extends Serializable {
  def annotateByVariant(dict: Broadcast[RefGene]): Variant[A] = {
    val variation = v.toVariation()

    val annot = IntervalTree.lookup(dict.value.loci, variation).map{l =>
      (l.geneName, l.mRNAName, l.annotate(variation, dict.value.seq(l.mRNAName)))}
    annot match {
      case Nil =>
        //logger.warn(s"no annotation for variant ${variation.toString}")
        v
      case _ =>
        val consensus = annot.map(p => (p._1, p._3))
          .reduce((a, b) => if (FM(a._2) < FM(b._2)) a else b)
        val merge = annot.map(p => (p._1, s"${p._2}:${p._3.toString}")).groupBy(_._1)
          .map{case (k, value) => "%s::%s".format(k, value.map(x => x._2).mkString(","))}
          .mkString(",,")
        v.addInfo(IK.anno, merge)
        v.addInfo(IK.gene, consensus._1)
        v.addInfo(IK.func, consensus._2.toString)
        v
    }
  }

  def annotateByGene(dict: Broadcast[RefGene]): Array[Variant[A]] = {
    /** the argument RefGene is the whole set of all genes involved
      * the output RefGene each represents a single gene
      * */
    //val point = Region(s"${v.chr}:${v.pos}-${v.pos.toInt + 1}").asInstanceOf[Single]

    val variation = v.toVariation()

    val all = IntervalTree.lookup(dict.value.loci, variation)
      .map(l => (l.geneName, l.annotate(variation, dict.value.seq(l.mRNAName))))
      .groupBy(_._1).mapValues(x => x.map(_._2).reduce((a, b) => if (FM(a) < FM(b)) a else b))
      .toArray.map{x =>
      val newV = v.copy
      newV.addInfo(IK.gene, x._1)
      newV.addInfo(IK.func, x._2.toString)
      newV
    }
    all
  }

  def annotateByRegion(windowSize: Int, overlapSize: Int): Array[Variant[A]] = {
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
      v.addInfo("Region", startRegion.toString)
      Array(v)
    } else {
      val v2 = v.copy
      v.addInfo("Region", startRegion.toString)
      v2.addInfo("Region", endRegion.toString)
      Array(v, v2)
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
}

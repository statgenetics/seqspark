package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.dizhang.seqspark.annot.{IntervalTree, Location, RefGene, dbNSFP}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.worker.WorkerObsolete._

import sys.process._

/**
 * Annotation pipeline
 */

object Annotation extends WorkerObsolete[Data, Data] {

  implicit val name = new WorkerName("annotation")

  val F = Constant.Annotation.Feature
  val FM = F.values.zipWithIndex.toMap
  val Nucleotide = Constant.Annotation.Base
  val IK = Constant.Variant.InfoKey

  type Genes = Map[String, List[Location]]

  def annotateByVariant[A](v: Variant[A], dict: Broadcast[RefGene]): Variant[A] = {
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

  def annotateByGene[A](v: Variant[A], dict: Broadcast[RefGene]): Array[Variant[A]] = {
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

  def forAssoc(input: VCF)(implicit cnf: RootConfig, sc: SparkContext): VCF = {
    val build = input.config.genomeBuild.toString
    val coordFile = cnf.annotation.geneCoord
    val seqFile = cnf.annotation.geneSeq
    val refGene = sc.broadcast(RefGene(build, coordFile, seqFile))
    input match {
      case ByteGenotype(vs, c) =>
        val anno = vs.map(v => annotateByGene(v, refGene)).flatMap(x => x)
        ByteGenotype(anno, c)
      case StringGenotype(rvs, c) =>
        val anno = rvs.map(v => annotateByGene(v, refGene)).flatMap(x => x)
        StringGenotype(anno, c)
    }
  }

  def joinDbNSFP(input: VCF)(implicit cnf: RootConfig, sc: SparkContext): VCF = {
    val annoConf = cnf.annotation
    val db = dbNSFP(annoConf.dbNSFP)
    input match {
      case ByteGenotype(vs, c) =>
        val pair = vs.map(v => (v.toVariation(), v)).leftOuterJoin(db.zipWithKey(c.genomeBuild))
        val anno = pair.map{p =>
          if (p._2._2.isDefined) {
            p._2._2.get.foreach((k, v) => p._2._1.addInfo(k, v))
          }
          p._2._1
        }
        ByteGenotype(anno, c)
      case StringGenotype(rvs, c) =>
        val pair = rvs.map(v => (v.toVariation(), v)).leftOuterJoin(db.zipWithKey(c.genomeBuild))
        val anno = pair.map{p =>
          if (p._2._2.isDefined) {
            p._2._2.get.foreach((k ,v ) => p._2._1.addInfo(k, v))
          }
          p._2._1
        }
        StringGenotype(anno, c)
    }
  }

  def apply(input: Data)(implicit cnf: RootConfig, sc: SparkContext): Data = {
    logger.info("start annotation")
    try {
      val exitCode = s"mkdir -p $workerDir".!
      logger.info(s"workerDir '$workerDir' created successfully")
    } catch {
      case e: Exception =>
        logger.error(s"failed to create workerDir '$workerDir', exit")
        System.exit(1)
    }
    val build = input._1.config.genomeBuild.toString
    val coordFile = cnf.annotation.geneCoord
    val seqFile = cnf.annotation.geneSeq
    val refGene = sc.broadcast(RefGene(build, coordFile, seqFile))
    logger.info(s"${IntervalTree.count(refGene.value.loci)} locations in refgene")
    input match {
      case (ByteGenotype(vs, c), p) =>
        val anno = vs.map(v => annotateByVariant(v, refGene))
        (ByteGenotype(anno, c), p)
      case (StringGenotype(rvs, c), p) =>
        val anno = rvs.map(v => annotateByVariant(v, refGene))
        (StringGenotype(anno, c), p)
    }
  }
}

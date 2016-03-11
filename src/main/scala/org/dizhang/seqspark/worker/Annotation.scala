package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.dizhang.seqspark.annot.{IntervalTree, Location, RefGene}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.worker.Worker._
import sys.process._


/**
 * Annotation pipeline
 */

object Annotation extends Worker[Data, Data] {

  implicit val name = new WorkerName("annotation")

  val F = Constant.Annotation.Feature
  val FM = F.values.zipWithIndex.toMap
  val Nucleotide = Constant.Annotation.Nucleotide
  val IK = Constant.Variant.InfoKey

  type Genes = Map[String, List[Location]]

  def annotate[A](v: Variant[A], dict: Broadcast[RefGene]): Array[Variant[A]] = {
    /** the argument RefGene is the whole set of all genes involved
      * the output RefGene each represents a single gene
      * */
    val point = Region(s"${v.chr}:${v.pos}-${v.pos.toInt + 1}").asInstanceOf[Single]

    val variation = v.toVariation

    val all = IntervalTree.lookup(dict.value.loci, point)
      .map(l => (l.geneName, l.annotate(variation, dict.value.seq(l.mRNAName))))
      .groupBy(_._1).mapValues(x => x.map(_._2).reduce((a, b) => if (FM(a) < FM(b)) a else b))
      .toArray.map{x => v.addInfo(IK.gene, x._1); v.addInfo(IK.func, x._2.toString); v}
    all
  }

  def forAssoc(input: VCF)(implicit cnf: RootConfig, sc: SparkContext): VCF = {
    val build = input.config.genomeBuild.toString
    val coordFile = cnf.annotation.geneCoord
    val seqFile = cnf.annotation.geneSeq
    val refGene = sc.broadcast(RefGene(build, coordFile, seqFile))
    input match {
      case ByteGenotype(vs, c) =>
        val anno = vs.map(v => annotate(v, refGene)).flatMap(x => x)
        ByteGenotype(anno, c)
      case StringGenotype(rvs, c) =>
        val anno = rvs.map(v => annotate(v, refGene)).flatMap(x => x)
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
    input match {
      case (ByteGenotype(vs, c), p) =>
        val anno = vs.map(v => annotate(v, refGene)).flatMap(x => x)
        (ByteGenotype(anno, c), p)
      case (StringGenotype(rvs, c), p) =>
        val anno = rvs.map(v => annotate(v, refGene)).flatMap(x => x)
        (StringGenotype(anno, c), p)
    }
  }
}

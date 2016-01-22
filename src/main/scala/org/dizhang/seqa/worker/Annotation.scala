package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.annot.{IntervalTree, Location, RefGene}
import org.dizhang.seqa.ds.{Region, Single, Variant}
import org.dizhang.seqa.util.{Constant, Command}
import org.dizhang.seqa.util.InputOutput._
import scala.collection.immutable.TreeSet
import sys.process._

import scala.io.Source

/**
 * Annotation pipeline
 */

object Annotation extends Worker[VCF, AnnoVCF] {

  implicit val name = new WorkerName("annotation")

  val CP = Constant.ConfigPath.Annotation
  val CV = Constant.ConfigValue.Annotation
  val F = Constant.Annotation.Feature
  val FM = F.values.zipWithIndex.toMap
  val Nucleotide = Constant.Annotation.Nucleotide

  type Genes = Map[String, List[Location]]

  def annotate(v: Var, dict: Broadcast[RefGene]): Array[(String, (F.Feature, Var))] = {
    /** the argument RefGene is the whole set of all genes involved
      * the output RefGene each represents a single gene
      * */
    val point = Region(s"${v.chr}:${v.pos}-${v.pos.toInt + 1}").asInstanceOf[Single]
    val all = IntervalTree.lookup(dict.value.loci, point)
      .map(l => (l.geneName, l.annotate(point, Some(dict.value.seq(l.mRNAName)), Some(Nucleotide.withName(v.alt)))))
      .groupBy(_._1).mapValues(x => x.map(_._2).reduce((a, b) => if (FM(a) < FM(b)) a else b))
      .toArray.map(x => (x._1, (x._2, v)))
    all
  }

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): AnnoVCF = {
    logger.info("start annotation")
    try {
      val exitCode = s"mkdir -p $workerDir".!
      logger.info(s"workerDir '$workerDir' created successfully")
    } catch {
      case e: Exception =>
        logger.error(s"failed to create workerDir '$workerDir', exit")
        System.exit(1)
    }
    val build = cnf.getString(CP.RefGene.build)
    val coordFile = cnf.getString(CP.RefGene.coord)
    val seqFile = cnf.getString(CP.RefGene.seq)
    val refGene = sc.broadcast(RefGene(build, coordFile, seqFile))
    val anno = input.map(v => annotate(v, refGene)).flatMap(x => x)
    anno
  }

}

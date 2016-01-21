package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.annot.{Location, RefGene}
import org.dizhang.seqa.ds.{Region, Single, Variant}
import org.dizhang.seqa.util.{Constant, Command}
import org.dizhang.seqa.util.InputOutput._
import scala.collection.immutable.TreeSet
import sys.process._

import scala.io.Source

/**
 * Annotation pipeline
 */

object Annotation extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("annotation")

  val CP = Constant.ConfigPath.Annotation
  val CV = Constant.ConfigValue.Annotation

  type Genes = Map[String, TreeSet[Location]]
  def addGenes(a: Genes, b: Genes): Genes = {
    a ++ (for ((k, v) <- b) yield k -> (v ++ a.getOrElse(k, TreeSet[Location]())))
  }

  def lookupGene(v: Var, dict: Broadcast[RefGene]): Array[(RefGene, Var)] = {
    /** the argument RefGene is the whole set of all genes involved
      * the output RefGene each represents a single gene
      * */
    val point = Region(s"${v.chr}:${v.pos}-${v.pos.toInt + 1}")
    val all = dict.value.loci.filter(l => l overlap point)
      .map(l => Map(l.geneName -> TreeSet(l)))
      .reduce((a, b) => addGenes(a, b))
    all.toArray.map{
      case (g, t) =>
        (new RefGene(dict.value.build, t, t.map(l => l.mRNAName).map(n => (n, dict.value.seq(n))).toMap), v)
    }
  }

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
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
    input.map(v => lookupGene(v, refGene)).flatMap(x => x)


  }
/**
  * use annovar, deprecated
  *
  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    val exitCode = ("mkdir -p %s" format workerDir).!
    println(exitCode)
    val rawSites = workerDir + "/sites.raw.vcf"
    val annotatedSites = "sites.annotated"
    writeRDD(input.map(_.meta.slice(0, 8).mkString("\t") + "\n"), rawSites)
    Command.annovar(rawSites, annotatedSites, workerDir)
    val annot = sc.broadcast(readAnnot(annotatedSites))
    val funcAnnoted = input.zipWithIndex().map{
      case (v, i: Long) => {
      val meta = v.meta.clone()
      meta(7) =
        if (meta(7) == ".")
          annot.value(i.toInt)
        else
          "%s;%s" format (meta(7), annot.value(i.toInt))
      v.updateMeta(meta)
      }
    }

    if (cnf.getBoolean("annotation.maf.use")) mafAnnot(funcAnnoted) else funcAnnoted
  }

  def mafAnnot(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {

    val dbFile = cnf.getString("annotation.maf.source")
    /** Chromosome counts */
    val an = cnf.getString("annotation.maf.an")
    /** Alternative allele counts */
    val ac = cnf.getString("annotation.maf.ac")
    /** Mark which source the maf comes from */
    val tag = cnf.getString("annotation.maf.tag")

    def getKeyFromVariant[A](v: Variant[A]): String =
      s"${v.chr}-${v.pos}-${v.ref}-${v.alt}"

    def getKVPairsFromVCFRawLine(l: String): Array[(String, Double)] = {
      val s = l.split("\t")
      val alts = s(4).split(",")
      val info =
        (for {
          item <- s(7).split(";")
          s = item.split("=")
        } yield if (s.length == 1) s(0) -> "true" else s(0) -> s(1)).toMap
      val afs = info(ac).split(",").map(x => x.toDouble/info(an).toDouble)
      alts.zip(afs).map(a => (s"${s(0)}-${s(1)}-${s(3)}-${a._1}", a._2))
    }

    val db: RDD[(String, Double)] = sc.textFile(dbFile).map(l => getKVPairsFromVCFRawLine(l)).flatMap(x => x)

    val annoted: VCF = input.map(v => (getKeyFromVariant(v), v)).leftOuterJoin(db).map{
      case (k, (v, None)) => {v.addInfo(s"${tag}_AF", "NA"); v}
      case (k, (v, Some(af))) => {v.addInfo(s"${tag}_AF", af.toString); v}
    }
    annoted
  }

  def readAnnot(sites: String)(implicit cnf: Config): Array[String] = {
    val varFile = "%s/%s.variant_function" format (workerDir, sites)
    val exonFile = "%s/%s.exonic_variant_function" format (workerDir, sites)
    val varArr: Array[(String, String)] =
      (for {
        l <- Source.fromFile(varFile).getLines()
        s = l.split("\t")
      } yield (s(0), s(1))).toArray

    val exonMap: Map[Int, String] =
      (for {
        l <- Source.fromFile(exonFile).getLines()
        s = l.split("\t")
      } yield s(0).substring(4).toInt -> s(1))
        .toMap
    varArr.zipWithIndex.map{case ((a, b), i: Int) =>
      if (a == "exonic")
        "ANNO=exonic:%s;GROUP=%s" format (exonMap(i + 1), b)
      else
        "ANNO=%s;GROUP=%s" format (a, b)}
  }
  */
}

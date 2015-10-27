package org.dizhang.seqa.worker

import java.io.{File, FileOutputStream, IOException, PrintWriter}

import breeze.linalg.{SparseVector, Vector}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqa.ds.{Counter, Bed}
import org.dizhang.seqa.util.Constant
import Constant.{Unphased, Hg19, Hg38}
import Unphased.Bt
import org.dizhang.seqa.util.InputOutput._

import sys.process._
import scala.io.Source

/**
 * Sample level QC
 */
object SampleLevelQC extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("sample")

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    checkSex(input)

    mds(input)

    val pheno = cnf.getString("sampleInfo.source")
    val newPheno = workerDir + "/" + pheno.split("/").last
    val samples = readColumn(pheno, "IID")
    val removeCol = cnf.getString("sampleInfo.filter")
    val remove =
      if (hasColumn(pheno, removeCol))
        readColumn(pheno, removeCol)
      else
        Array.fill(samples.length)("0")

    /** save new pheno file after remove bad samples */
    val phenoArray = Source.fromFile(pheno).getLines.toArray
    val header = phenoArray(0)
    val newPhenoArray =
      for {
        i <- (0 until remove.length).toArray
        if (remove(i) != "1")
      } yield phenoArray(i + 1)
    writeArray(newPheno, header +: newPhenoArray)

    val maskMap: Map[Int, Byte] = remove
        .zipWithIndex
        .filter(p => p._1 == "1")
        .map(p => p._2 -> Bt.mis)
        .toMap

    val res = input.map(v => v.update(maskMap))
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (cnf.getBoolean("sampleLevelQC.save") == true)
      try {
        res.saveAsObjectFile(workerDir)
      } catch {
        case e: Exception => {println("step2: save failed"); System.exit(1)}
      }
    res
  }

  /** Check the concordance of the genetic sex and recorded sex */
  def checkSex(vars: VCF)(implicit cnf: Config) {
    /** Assume:
      *  1. genotype are cleaned before.
      *  2. only SNV
      * */

    /** count heterozygosity rate */
    def makex (g: Byte): Pair = {
      if (g == Bt.mis)
        (0, 0)
      else if (g == Bt.het)
        (1, 1)
      else
        (0, 1)
    }

    /** count the call rate */
    def makey (g: Byte): Pair =
      if (g == Bt.mis) (0, 1) else (1, 1)

    /** count for all the SNVs on allosomes */
    def count (vars: VCF): (Counter[Pair], Counter[Pair]) = {
      val pXY =
        if (cnf.getString("genotypeInput.genomeBuild") == "hg19")
          Hg19.pseudo
        else
          Hg38.pseudo
      val allo = vars
        .filter (v => v.chr == "X" || v.chr == "Y")
        .filter (v => pXY forall (r => ! r.contains(v.chr, v.pos.toInt - 1)) )
      allo.cache
      //println("The number of variants on allosomes is %s" format (allo.count()))
      val xVars = allo filter (v => v.chr == "X")
      val yVars = allo filter (v => v.chr == "Y")
      val emp: Counter[Pair] = Counter.fill[Pair](vars.take(1).length)((0, 0))
      val xHetRate =
        if (xVars.isEmpty)
          emp
        else {
          xVars
            .map(v => v.toCounter(makex))
            .reduce((a, b) => a ++ b)
          //peek(tmp)
          //val testVar = xVars.takeSample(false, 100)
          //val testCnt = testVar.map(v => Counter[Byte, Pair](v, makex))
          //writeArray("Var", testVar.map(_.toString))
          //writeArray("Cnt", testCnt.map(_.cnt.mkString("\t")))
          //val sumCnt = testCnt.map(_.cnt).reduce((a, b) => Counter.addGeno(a, b))
          //writeAny("Sum", sumCnt.mkString("\t"))
          //tmp.map(c => c.cnt).reduce((a, b) => Counter.addGeno[Pair](a, b))
        }
      val yCallRate =
        if (yVars.isEmpty)
          emp
        else
          yVars
            .map (v => v.toCounter(makey))
            .reduce ((a, b) => a ++ b)
      (xHetRate, yCallRate)
    }

    /** write the result into file */
    def write (res: (Counter[Pair], Counter[Pair])) {
      val pheno = cnf.getString("sampleInfo.source")
      val fids = readColumn(pheno, "FID")
      val iids = readColumn(pheno, "IID")
      val sex = readColumn(pheno, "Sex")
      val prefDir = workerDir
      val exitCode = "mkdir -p %s".format(prefDir).!
      println(exitCode)
      val outFile = "%s/sexCheck.csv" format (prefDir)
      val pw = new PrintWriter(new File(outFile))
      pw.write("fid,iid,sex,xHet,xHom,yCall,yMis\n")
      val x: Counter[Pair] = res._1
      val y: Counter[Pair] = res._2
      for (i <- 0 until iids.length) {
        pw.write("%s,%s,%s,%d,%d,%d,%d\n" format (fids(i), iids(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
      }
      pw.close
    }
    write(count(vars))
  }

  /** run the global ancestry analysis */
  def mds (vars: VCF)(implicit cnf: Config, sc: SparkContext) {
    /**
     * Assume:
     *     1. genotype are cleaned before
     *     2. only SNVs
     */
    val pheno = cnf.getString("sampleInfo.source")
    val mdsMaf = cnf.getString("sampleLevelQC.mdsMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m >= mdsMaf && m <= (1 - mdsMaf)) true else false

    val snp = VariantLevelQC.miniQC(vars, pheno, mafFunc)
    //saveAsBed(snp, ini, "%s/9external/plink" format (ini.get("general", "project")))
    //println("\n\n\n\tThere are %s snps for mds" format(snp.count()))
    val bed = snp map (s => Bed(s))
    //println("\n\n\n\tThere are %s beds for mds" format(bed.count()))
    def write (bed: RDD[Bed]) {
      val pBed =
        bed mapPartitions (p => List(p reduce ((a, b) => Bed.add(a, b))).iterator)
      /** this is a hack to force compute all partitions*/
      pBed.cache()
      pBed foreachPartition (p => None)
      val prefix = workerDir
      val bimFile = "%s/all.bim" format (prefix)
      val bedFile = "%s/all.bed" format (prefix)
      var bimStream = None: Option[FileOutputStream]
      var bedStream = None: Option[FileOutputStream]
      val iter = pBed.toLocalIterator
      try {
        bimStream = Some(new FileOutputStream(bimFile))
        bedStream = Some(new FileOutputStream(bedFile))
        val bedMagical1 = Integer.parseInt("01101100", 2).toByte
        val bedMagical2 = Integer.parseInt("00011011", 2).toByte
        val bedMode = Integer.parseInt("00000001", 2).toByte
        val bedHead = Array[Byte](bedMagical1, bedMagical2, bedMode)
        bedStream.get.write(bedHead)
        while (iter.hasNext) {
          val cur = iter.next
          bimStream.get.write(cur.bim)
          bedStream.get.write(cur.bed)
        }
      } catch {
        case e: IOException => e.printStackTrace
      } finally {
        if (bimStream.isDefined) bimStream.get.close
        if (bedStream.isDefined) bedStream.get.close
      }
      pBed.unpersist()
    }
    write(bed)
    /**
    val f: Future[String] = future {
      Command.popCheck(ini: Ini)
    }
    f onComplete {
      case Success(m) => println(m)
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
      */
  }
}

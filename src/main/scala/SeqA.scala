/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.collection.mutable.Map
import java.io._
import java.nio.file.{Paths, Files}
import org.ini4j._
import Utils._
import Pipeline._

object SeqA {
  def checkArgs(args: Array[String]): Boolean = {
    val usage = """
                |spark-submit --class SeqA [options] /path/to/wesqc.xx.xx.jar seqa.conf [steps]
                |    options:     spark options, e.g. --num-executors
                |    seqa.conf:  the configuation file in INI format, could be other name
                |    steps:       the QC steps to perform, default 0-3
                |                 0: input from vcf
                |                 1: genotype level qc
                |                 2: sample level qc
                |                 3: variant level qc
                |"""

    if (args.length == 0 || args.length > 2) {
      println(usage)
      false
    } else if (args.length == 2) {
      val p1 = "([0-4])".r
      val p2 = "([0-4])-([0-4])".r
      args(1) match {
        case p1(s) => true
        case p2(s1, s2) if (s2.toInt >= s1.toInt) => true
        case p2(s1, s2) if (s2.toInt < s1.toInt) => {println(usage); false}
        case _ => {println(usage); false}
      }
    } else
      true
  }

  def checkConf(ini: Ini) {
    println("Conf file fine")
  }


  def main(args: Array[String]) {
    /** check args */
    if (! checkArgs(args)) {
      System.exit(1)
    }

    /** quick run */
    val conf = new File(args(0))
    try {
      val ini = new Ini(conf)
      val steps = if (args.length == 2) args(1) else "0-3"
      checkConf(ini)
      quickRun(ini, steps)
    } catch {
      case e: Exception => {e.printStackTrace()}
    }

    //} catch {
    //  case ex: FileNotFoundException => System.exit(1)
    //}

    //val vcf = sc.textFile(ini.get("general", "vcf"))
    //val pheno = ini.get("general", "pheno")
    //val vars = makeVariants(vcf, ini)
    //vcf filter (line => ! line.startsWith("#")) map (line => Variant(line))
    //vars.cache()
    
    //saveAsBed(vars, ini, sc)

    //varCountByFreqAndFunc(vars, "input/annotVar.csv", "input/freqVar.csv", ini)

    //checkSex(vars, pheno, ini)

    //println("project: %s" format (ini.get("general", "project")))

    

    /**
    val vcfFile = if (args.length >= 1) args(0) else "test.vcf.bz2"
    val headFile = "mhgrid_head"
    val refPattern = """^[ATCG]$"""
    val altPattern = refPattern
    val initFilters =
      List(
        "VQSRTrancheSNP99.00to99.90",
        "VQSRTrancheSNP99.90to100.00",
        "VQSRTrancheSNP99.90to100.00+",
        "GenoLowQual",
        "HighMis",
        "HighMisBatch",
        "BatchSpec",
        "HWE",
        "PASS")
    val afterVQSR = initFilters.filterNot(f => f.matches("^VQSR.*"))
    val afterGLQC = afterVQSR.filterNot(f => f == "GenoLowQual")
    val afterMis = afterGLQC.filterNot(f => f == "HighMis")
    val afterMisB = afterMis.filterNot(f => f == "HighMisBatch")
    val afterBspec = afterMisB.filterNot(f => f == "BatchSpec")
    val afterHWE = List("PASS")
    val testData = sc.textFile(vcfFile)

    def sampleId(file: String): Array[String] = {
      val allLines = Source.fromFile(file).getLines.toList
      //val allLines = sc.textFile(file).collect
      val line = allLines.filter(x => x.startsWith("#CHROM"))(0).split("\t")
      //println(line)
      line.slice(9, line.length)
    }

    //val ids = sampleId(headFile)
    def filterLine(line: String): Boolean = {
      if (line.startsWith("#"))
        false
      else {
        val left = line.drop(0).take(100).split("\\t")
        if (left(3).length == 1 && left(4).length == 1 && initFilters.contains(left(6)))
          true
        else
          false}}
    //val vars = testData.filter(filterLine).map(line => Variant(line)).cache()
    val vars = testData.filter(line => ! line.startsWith("#")).map(line => Variant(line)).cache()
    def readBatch(file: String): Array[Int] = {
      val allLines = Source.fromFile(file).getLines.toList
      allLines.map(x => x.split(":")(1).toInt).toArray
    }
    val batch = readBatch("batch")

    def filterVariant(variant: Variant[String], filters: List[String]): Boolean = {
      if (filters.contains(variant.filter)) true
      else false}

    //val biAutoSnv = vars filter (v => afterVQSR.contains(v.filter))


    val espIds = sampleId("espHead")

    callRate(vars, "espCallRate", espIds)
      */

    //val intersectSnv =
    //inter(biAutoSnv, batch)
    //countByGD(intersectSnv, "cntInterByGD.txt", batch)

    //countByGD(biAutoSnv, "cntByGD.txt", batch)

    //countByFilter(biAutoSnv, "biAutoSnv.cnts.txt")

    //val cnts: RDD[(String, Int)] =
    //  biAutoSnv map (v => (v.filter, 1)) reduceByKey ((a: Int, b: Int) => a + b)
    //cnts foreach {case (f: String, c: Int) => println(f + ": " + c)}

    //biAutoSnv.saveAsObjectFile("testObj")


    //val maf1 = computeMaf(vars, true).map(identity)

    /**
      * val maf2 = computeMaf(vars, false).map(identity)
      /*
      * val pw = new PrintWriter(new File("mafQc.txt"))
      * pw.write("chr\tpos\tmaf\tgroup\n")
      * vars.map(v => (v.chr, v.pos, v.filter)).collect.foreach{x =>  pw.write("%s\t%s\t%s\t%s\n" format(x._1, x._2, maf1((x._1, x._2)), x._3))}
      * pw.close*/
      * val pw2 = new PrintWriter(new File("mafRaw.txt"))
      * pw2.write("chr\tpos\tmaf\tgroup\n")
      * vars.map(v => (v.chr, v.pos, v.filter)).collect.foreach{x =>  pw2.write("%s\t%s\t%s\t%s\n" format(x._1, x._2, maf2((x._1, x._2)), x._3))}
      * pw2.close

     */
    //computeMis(vars, "MisRateRaw.txt", false, maf)

    //computeMis(vars.filter(v => filterVariant(v, afterVQSR)), "MisRateQc.txt", true, maf)
    //computeGQ(vars, "GqByMaf.txt", ids, maf)
    //computeGQ(vars, "GqByGt.txt", ids)
    //computeGQ(vars.filter(v => afterVQSR.contains(v.filter)), "raw")
    //computeTitv(vars, "All")
    //computeTitv(vars.filter(x => x.info.contains("DB")), "Known")
    //computeTitv(vars.filter(x => ! x.info.contains("DB")), "Novel")
    //computeGenoCount
  }
}

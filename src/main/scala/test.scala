/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.io.Source
import java.io._
import scala.collection.mutable.Map

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test VCF")
    val sc = new SparkContext(conf)

    val vcfFile = if (args.length >= 1) args(0) else "test.vcf.bz2"// Should be some file on your system
    val headFile = "mhgrid_head"
    val refPattern = """^[ATCG]$"""
    val altPattern = refPattern
    val initFilters = List("VQSRTrancheSNP99.00to99.90", "VQSRTrancheSNP99.90to100.00", "VQSRTrancheSNP99.90to100.00+", "GenoLowQual", "HighMis", "HighMisBatch", "BatchSpec", "HWE", "PASS")
    val afterVQSR = initFilters.filterNot(f => f.matches("^VQSR"))
    val afterGLQC = afterVQSR.filterNot(f => f == "GenoLowQual")
    val afterMis = afterGLQC.filterNot(f => f == "HighMis")
    val afterMisB = afterMis.filterNot(f => f == "HighMisBatch")
    val afterBspec = afterMisB.filterNot(f => f == "BatchSpec")
    val afterHWE = List("PASS")
    val testData = sc.textFile(vcfFile)

    def sampleId(file: String): Array[String] = {
      //val allLines = Source.fromFile(file).getLines.toList
      val allLines = sc.textFile(file).collect
      val line = allLines.filter(x => x.startsWith("#CHROM"))(0).split("\t")
      //println(line)
      line.slice(9, line.length)
    }
    val ids = sampleId(headFile)
    def filterLine(line: String): Boolean = {
      if (line.startsWith("#"))
        false
      else {
        val left = line.drop(0).take(100).split("\\t")
        if (left(3).length == 1 && left(4).length == 1 && initFilters.contains(left(6)))
          true
        else
          false}}
    val vars = testData.filter(filterLine).map(line => Variant(line)).cache()
    def readBatch(file: String): Array[Int] = {
      val allLines = Source.fromFile(file).getLines.toList
      allLines.map(x => x.split(":")(1).toInt).toArray
    }
    val batch = readBatch("batch")

    def filterVariant(variant: Variant, filters: List[String]): Boolean = {
      if (filters.contains(variant.filter)) true
      else false}

    def addTitv(a: (Array[Int], Array[Int]), b: (Array[Int], Array[Int])): (Array[Int], Array[Int]) = {
      for (i <- Range(0, a._1.length)) {
        a._1(i) += b._1(i)
        a._2(i) += b._2(i)}
      a
    }

    def writeTitv(titv: (Array[Int], Array[Int]), file: String) {
      val pw = new PrintWriter(new File(file))
      for (i <- Range(0, titv._1.length)) {
        pw.write(titv._1(i) + " " + titv._2(i) + " " + titv._1(i) * 1.0 / titv._2(i) + "\n")
      }
      pw.close
      println(file + " done")
    }

    def writeTitvs(tag: String, titvs: Array[(String, (Array[Int], Array[Int]))]) {
      for (titv <- titvs) {
        writeTitv(titv._2, tag + "_" + titv._1 + ".txt")
      }
    }

    def writeCombinedTitvs(tag: String, titvs: Array[(String, (Array[Int], Array[Int]))]) {
      val pw = new PrintWriter(new File(tag + "_titv.txt"))
      pw.write("id")
      titvs.map(x => pw.write("\t" + x._1 + "_ti" + "\t" + x._1 + "_tv"))
      pw.write("\n")
      //println(titvs(0)._2._1.length)
      for (i <- Range(0, titvs(0)._2._1.length)) {
        pw.write(ids(i))
        titvs.map(x => pw.write("\t" + x._2._1(i) + "\t" + x._2._2(i)))
        pw.write("\n")
      }
      pw.close
    }

    def computeTitv(vars: RDD[Variant], tag: String) {
      val rawTitvs = vars.map(x => (x.filter, x.toTitv(false))).reduceByKey((a, b) => addTitv(a, b)).collect
      val cleanTitvs = vars.map(x => (x.filter, x.toTitv(true))).reduceByKey((a, b) => addTitv(a, b)).collect
      writeCombinedTitvs("raw" + tag, rawTitvs)
      writeCombinedTitvs("clean" + tag, cleanTitvs)
    }
    //computeTitv(vars, "All")
    computeTitv(vars.filter(x => x.info.contains("DB")), "Known")
    computeTitv(vars.filter(x => ! x.info.contains("DB")), "Novel")

    def computeGenoCount {
      def toGD(gs: Array[String], gq: Double): Map[Int, Int] = {
        val gdMap = Map[Int, Int]()
        for (gt <- gs) {
          val g = gt.split(":")
          if (g(0) == "./." || g(4).toDouble < gq || g(3).toInt > 249) {
            if (gdMap.contains(0))
              gdMap(0) += 1
            else
              gdMap(0) = 1
          }
          else {
            if (gdMap.contains(g(3).toInt))
              gdMap(g(3).toInt) += 1
            else
              gdMap(g(3).toInt) = 1
          }
        }
        gdMap
      }
      def toGQ(gs: Array[String], gd: Int): Map[Int, Int] = {
        val gqMap = Map[Int, Int]()
        for (gt <- gs) {
          val g = gt.split(":")
          if (g(0) == "./." || g(3).toInt < gd || g(3).toInt > 249) {
            if (gqMap.contains(-1))
              gqMap(-1) += 1
            else
              gqMap(-1) = 1
          }
          else {
            if (gqMap.contains(g(4).toInt))
              gqMap(g(4).toInt) += 1
            else
              gqMap(g(4).toInt) = 1
          }
        }
        gqMap
      }

      def toMap(variant: Variant, gx: String, cutoff: Int): scala.collection.immutable.Map[Int, Map[Int, Int]] = {
        val funGX = if (gx == "gd") toGD(_ : Array[String], cutoff) else toGQ(_ : Array[String], cutoff)
        variant.geno.zipWithIndex.groupBy{case (g, i) => batch(i)}.mapValues(g => funGX(g.map(x => x._1))).map(identity)
      }

      def addGX(a: Map[Int, Int], b: Map[Int, Int]): Map[Int, Int] = {
        for (key <- a.keys ++ b.keys) {
          if (a.contains(key) && b.contains(key)) {
            a(key) += b(key)
          } else if (b.contains(key)) {
            a(key) = b(key)
          }
        }
        a
      }

      def addMap(a: scala.collection.immutable.Map[Int, Map[Int, Int]], b: scala.collection.immutable.Map[Int, Map[Int, Int]]): scala.collection.immutable.Map[Int, Map[Int, Int]] = {
        for (i <- 1 to 4){
          addGX(a(i), b(i))
        }
        a
      }

      //val rawGD = vars.filter(x => afterVQSR.contains(x.filter)).map(x => toMap(x, "gd", 0)).reduce((a, b) => addMap(a, b))
      val rawGQ = vars.filter(x => afterVQSR.contains(x.filter)).map(x => toMap(x, "gq", 0)).reduce((a, b) => addMap(a, b))
      //val cleanGD = vars.filter(x => afterVQSR.contains(x.filter)).map(x => toMap(x, "gd",20)).reduce((a, b) => addMap(a, b))
      //val cleanGQ = vars.filter(x => afterVQSR.contains(x.filter)).map(x => toMap(x, "gq",8)).reduce((a, b) => addMap(a, b))
      def writeGX(file: String, gx: scala.collection.immutable.Map[Int, Map[Int, Int]]) {
        val pw = new PrintWriter(new File(file))
        pw.write("cutoff\tbatch1\tbatch2\tbatch3\tbatch4\n")
        gx(1).keys.toList.sorted.map(x => {pw.write(x + "\t" + Range(1,5).map(i => gx(i)(x)).mkString("\t") + "\n")})
        pw.close
      }
      //writeGX("rawGD.txt", rawGD)
      writeGX("rawGQ.txt", rawGQ)
      //writeGX("cleanGD.txt", cleanGD)
      //writeGX("cleanGQ.txt", cleanGQ)
    }
    //computeGenoCount
  }
}

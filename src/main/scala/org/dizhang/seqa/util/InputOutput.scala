package org.dizhang.seqa.util

import java.io._
import com.typesafe.config.Config
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.spark.rdd.RDD
import Constant._
import org.dizhang.seqa.ds.Variant
import scala.io.Source
/**
 * defines some input/output functions here
 */

object InputOutput {
  type RawVar = Variant[String]
  type Var = Variant[Byte]
  type RawVCF = RDD[RawVar]
  type VCF = RDD[Var]
  type AnnoVCF = RDD[(String, (Constant.Annotation.Feature.Feature, Var))]
  type Pair = (Int, Int)

  /** Give Worker Object a name
    * use a class to make it convenient in implicit parameters */
  case class WorkerName(name: String) extends AnyVal {
    override def toString = name
  }

  /** If RDD[Variant[Byte]\] is required but RDD[Variant[String]\]
    * is provided, convert it implicitly */
  implicit def naivelyConvertRawVcfToVcf(raw: RawVCF)(implicit cnf: Config): VCF =
    raw.map(v => v.map(g => g.bt))

  def localWorkingDir(implicit cnf: Config): String =
    cnf.getString("localWorkingDir")

  def resultsDir(implicit cnf: Config): String = {
    "%s/results" format localWorkingDir
  }

  def workerDir(implicit cnf: Config, name: WorkerName): String = {
    "%s/%s" format (resultsDir, name)
  }

  def saveDir(implicit cnf: Config, name: WorkerName): String = {
    "%s/%s" format (cnf.getString("hdfsWorkingDir"), name)
  }

  def sitesFile(implicit cnf: Config, name: WorkerName): String = {
    "%s/sites.raw.vcf" format workerDir
  }

  def sampleSize(file: String): Int = {
    Source.fromFile(file).getLines().length - 1
  }

  def readColumn (file: String, col: String): Array[String] = {
    val delim = Pheno.delim
    val data = Source.fromFile(file).getLines.toList
    val header = data(0).split(delim).zipWithIndex.toMap
    val idx = header(col)
    val res =
      for (line <- data.slice(1, data.length))
      yield line.split(delim)(idx)
    res.toArray
  }

  def hasColumn (file: String, col: String): Boolean = {
    val delim = Pheno.delim
    val header = Source.fromFile(file).getLines.toList
    if (header(0).split(delim).contains(col))
      true
    else
      false
  }

  def writeArray(file: String, data: Array[String]) {
    val pw = new PrintWriter(new File(file))
    data foreach (d => pw.write(d + "\n"))
    pw.close()
  }

  def writeAny(file: String, data: String): Unit = {
    val pw = new PrintWriter(new File(file))
    pw.write(data + "\n")
    pw.close()
  }

  def getFam (file: String): Array[String] = {
    val delim = Pheno.delim
    val data = Source.fromFile(file).getLines.toArray
    val res =
      for (line <- data.drop(1))
      yield line.split(delim).slice(0, 6).mkString(delim)
    res
  }

  def writeRDD (data: RDD[String], file: String, head: String = ""): Unit = {
    val fw = new FileWriter(file)
    if (head != "")
      fw.write(head)
    data.foreach(v => Unit) //force compute
    val iterator = data.toLocalIterator
    while (iterator.hasNext) {
      val cur = iterator.next()
      fw.write(cur)
    }
    fw.close()
  }

  def peek[A](dat: RDD[A]): Unit = {
    println("There are %s records like this:" format(dat.count()))
    println(dat.takeSample(false,1)(0))
  }

  def compress(text: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    try {
      val out = new BZip2CompressorOutputStream(baos)
      out.write(text.getBytes())
      out.close()
    } catch {
      case ioe: IOException => throw ioe
      case e: Exception => throw e
    }
    baos.toByteArray
  }

}

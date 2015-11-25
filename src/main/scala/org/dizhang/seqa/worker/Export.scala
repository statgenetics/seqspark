package org.dizhang.seqa.worker

import java.io.{IOException, BufferedOutputStream, BufferedInputStream, FileOutputStream}

import com.typesafe.config.Config
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.ds.{Region, Variant}
import org.dizhang.seqa.util.Constant._
import UnPhased._
import org.dizhang.seqa.util.InputOutput._
import org.dizhang.seqa.ds
import sys.process._

/**
 * Export genotype data
 */

object Export extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("export")
  type Buffer = Array[Byte]

  def selectVariant(v: Variant[Byte], rs: Array[Region]): Boolean = {
    for (r <- rs) {
      if (r.contains(v.chr, v.pos.toInt))
        return true
    }
    false
  }

  def saveVCF(input: VCF, path: String, phased: Boolean): Unit = {
    val output: RDD[String] =
      if (phased)
        input.map(v => s"${v.meta.mkString("\t")}\t${v.geno(x => x.toPhased).mkString("\t")}")
      else
        input.map(v => s"${v.meta.mkString("\t")}\t${v.geno(x => x.toUnPhased).mkString("\t")}")
    output.saveAsTextFile(path)
  }

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    val phenoFile = cnf.getString("sampleInfo.source")
    val samplesCol = cnf.getString("export.samples")
    val samples: Array[Boolean] =
      if (hasColumn(phenoFile, samplesCol))
        readColumn(phenoFile, samplesCol).map(x => if (x == "1") true else false)
      else
        Array.fill(sampleSize(phenoFile))(true)
    val regions = cnf.getString("export.variants").split(",").map(p => Region(p))
    val output = input.filter(v => selectVariant(v, regions)).map(v => v.select(samples))
    val path = saveDir
    if (cnf.getString("export.type") == "vcf") {
      val phased = cnf.getBoolean("export.phased")
      saveVCF(output, saveDir, phased)
    }
    input
  }

}



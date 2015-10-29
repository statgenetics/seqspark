package org.dizhang.seqa.worker

import java.io.{IOException, BufferedOutputStream, BufferedInputStream, FileOutputStream}

import com.typesafe.config.Config
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.spark.SparkContext
import org.dizhang.seqa.ds.{Region, Variant}
import org.dizhang.seqa.util.Constant._
import Unphased._
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

  object HapMix {

    def makePhased(b: Byte): String = {
      b match {
        case Bt.ref => "00"
        case Bt.het1 => "01"
        case Bt.het2 => "10"
        case Bt.mut => "11"
        case Bt.mis => "99"
      }
    }

    def makeUnPhased(b: Byte): String = {
      b match {
        case Bt.ref => "0"
        case Bt.het1 => "1"
        case Bt.het2 => "1"
        case Bt.mut => "2"
        case Bt.mis => "9"
      }
    }

    def apply(input: VCF)(implicit cnf: Config): Unit = {
      val hm =
        if (cnf.getBoolean("export.phased"))
          input.map(v => ds.HapMix(v)(makePhased))
        else
          input.map(v => ds.HapMix(v)(makeUnPhased))
      val res = hm.mapPartitions{
          p => val x = p.reduce((a, b) => ds.HapMix.add(a, b))
          List((compress(x.snp), compress(x.geno))).toIterator
      }.collect()
      val snpFile = workerDir + "/hapmix.snp.bz2"
      val genoFile = workerDir + "/hapmix.geno.bz2"
      val fout1 = new FileOutputStream(snpFile)
      val fout2 = new FileOutputStream(genoFile)
      try {
        res.foreach {o => fout1.write(o._1); fout2.write(o._2)}
      } catch {
        case ioe: IOException => throw ioe
        case e: Exception => throw e
      } finally {
        fout1.close()
        fout2.close()
      }
    }
  }

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    val phenoFile = cnf.getString("sampleInfo.source")
    val samplesCol = cnf.getString("export.samples")
    val samples: Array[Boolean] = readColumn(phenoFile, samplesCol).map(x => if (x == "1") true else false)
    val regions = cnf.getString("export.variants").split(",").map(p => Region(p))
    val output = input.filter(v => selectVariant(v, regions)).map(v => v.select(samples)).repartition(input.partitions.length)
    if (cnf.getString("export.type") == "hapmix")
      HapMix(output)
    input
  }

}



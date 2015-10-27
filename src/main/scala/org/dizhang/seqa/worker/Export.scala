package org.dizhang.seqa.worker

import java.io.{BufferedOutputStream, BufferedInputStream, FileOutputStream}

import com.typesafe.config.Config
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.spark.SparkContext
import org.dizhang.seqa.ds.{Region, Variant}
import org.dizhang.seqa.util.Constant._
import org.dizhang.seqa.util.InputOutput._

/**
 * Export genotype data
 */

object Export extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("expot")
  type Buffer = Array[Byte]

  def selectVariant(v: Variant[Byte], rs: Array[Region]): Boolean = {
    for (r <- rs) {
      if (r.contains(v.chr, v.pos.toInt))
        return true
    }
    false
  }

  object HapMix {
    def variantToHapMix(v: Variant[Byte]): (Buffer, Buffer) = {
      val snp = s"${v.id} ${v.chr} ${v.pos.toFloat / 1e6 } ${v.pos} ${v.ref} ${v.alt}\n".getBytes()
      val flip = v.flip.getOrElse(false)
      implicit def make(b: Byte): String = {
        val x = if (flip) b else Phased.Bt.flip(b)
        x match {
          case Phased.Bt.ref => "00"
          case Phased.Bt.het1 => "01"
          case Phased.Bt.het2 => "10"
          case Phased.Bt.mut => "11"
        }
      }
      val geno = v.geno.mkString("") + "\n" getBytes()
      (snp, geno)
    }

    def apply(input: VCF)(implicit cnf: Config): Unit = {
      val hm = input.map(v => variantToHapMix(v))
      val bufSize = 4 * 1024 * 1024
      val fout1 = new FileOutputStream("%s/hapmix.snp" format workerDir)
      val fout2 = new FileOutputStream("%s/hapmix.geno" format workerDir)
      val out1 = new BufferedOutputStream(fout1, bufSize)
      val out2 = new BufferedOutputStream(fout2, bufSize)
      val bzOut1 = new BZip2CompressorOutputStream(out1)
      val bzOut2 = new BZip2CompressorOutputStream(out2)
      hm.cache()
      hm.foreach(_ => Unit)
      val iterator = hm.toLocalIterator
      while (iterator.hasNext) {
        val cur = iterator.next()
        bzOut1.write(cur._1)
        bzOut2.write(cur._2)
      }
      bzOut1.close()
      bzOut2.close()
      out1.close()
      out2.close()
      fout1.close()
      fout2.close()
    }
  }


  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {
    val phenoFile = cnf.getString("sampleInfo.source")
    val samplesCol = cnf.getString("export.samples")
    val samples: Array[Boolean] = readColumn(phenoFile, samplesCol).map(x => if (x == "1") true else false)
    val regions = cnf.getString("export.variants").split(",").map(p => Region(p))
    val output = input.filter(v => selectVariant(v, regions)).map(v => v.select(samples))
    if (cnf.getString("export.type") == "hapmix")
      HapMix(input)
    input
  }

}



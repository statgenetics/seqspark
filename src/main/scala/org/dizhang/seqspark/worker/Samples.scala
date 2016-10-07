package org.dizhang.seqspark.worker

import java.io.{File, PrintWriter}

import org.dizhang.seqspark.annot.IntervalTree
import org.dizhang.seqspark.ds.Counter
import org.dizhang.seqspark.ds.Counter._
import org.dizhang.seqspark.util.Constant.{Hg19, Hg38}
import org.dizhang.seqspark.util.SingleStudyContext
import org.dizhang.seqspark.util.UserConfig.GenomeBuild
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 9/20/16.
  */
object Samples {
  val logger = LoggerFactory.getLogger(getClass)

  def checkSex[A](self:Data[A], isHet: A => (Double, Double), callRate: A => (Double, Double))(ssc: SingleStudyContext): Unit = {
    val gb = ssc.userConfig.input.genotype.genomeBuild
    val pseudo = if (gb == GenomeBuild.hg19) {
      Hg19.pseudo
    } else {
      Hg38.pseudo
    }
    val chrX = self.filter(v =>
      (! IntervalTree.overlap(pseudo, v.toRegion)) && (v.chr == "X" | v.chr == "chrX")).cache()
    val chrY = self.filter(v =>
      (! IntervalTree.overlap(pseudo, v.toRegion)) && (v.chr == "Y" | v.chr == "chrY")).cache()
    val xHet: Option[Counter[(Double, Double)]] = if (chrX.count() == 0) {
      None
    } else {
      val res = chrX.map(v =>
        v.toCounter(isHet, (0.0, 1.0))).reduce((a, b) => a ++ b)
      Some(res)
    }
    val yCall: Option[Counter[(Double, Double)]] = if (chrY.count() == 0) {
      None
    } else {
      val res = chrY.map(v =>
        v.toCounter(callRate, (0.0, 1.0))).reduce((a, b) => a ++ b)
      Some(res)
    }
    val outdir = new File(ssc.userConfig.localDir + "/output")
    outdir.mkdir()
    logger.info("write checksex result")
    writeCheckSex((xHet, yCall), outdir.toString + "/checkSex.txt")(ssc)
    logger.info("done with checksex")
  }

  def writeCheckSex(data: (Option[Counter[(Double, Double)]], Option[Counter[(Double, Double)]]), outFile: String)
                   (ssc: SingleStudyContext): Unit = {

    logger.info(s"outFile is $outFile")
    val pheno = ssc.phenotype
    logger.info(s"phenotype ")
    val fid = pheno.select("fid").map(_.get)
    logger.info(s"fid ${fid.length}")
    val iid = pheno.select("iid").map(_.get)
    logger.info(s"fid ${iid.length}")
    val sex = pheno.select("sex").map{
      case None => "NA"
      case Some(s) => s
    }
    logger.info(s"sex ${sex.length}")
    val pw = new PrintWriter(new File(outFile))
    pw.write("fid,iid,sex,xHet,xHom,yCall,yMis\n")
    logger.info(s"before counter")
    val x: Counter[(Double, Double)] = data._1.getOrElse(Counter.fill(fid.length)((0.0, 0.0)))
    val y: Counter[(Double, Double)] = data._2.getOrElse(Counter.fill(fid.length)((0.0, 0.0)))
    logger.info(s"this should be the sample size: ${iid.length} ${x.length} ${y.length}")
    for (i <- iid.indices) {
      pw.write("%s,%s,%s,%f,%f,%f,%f\n" format (fid(i), iid(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
    }
    pw.close()
    logger.info("are we done now?")
  }
}

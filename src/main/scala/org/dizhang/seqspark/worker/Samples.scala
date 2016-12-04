package org.dizhang.seqspark.worker

import java.io.{File, PrintWriter}

import org.dizhang.seqspark.annot.IntervalTree
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Counter, Genotype, Phenotype}
import org.dizhang.seqspark.stat.PCA
import org.dizhang.seqspark.util.Constant.{Hg19, Hg38}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.GenomeBuild
import org.dizhang.seqspark.util.{LogicalParser, SingleStudyContext}
import org.slf4j.LoggerFactory


/**
  * Created by zhangdi on 9/20/16.
  */
object Samples {
  val logger = LoggerFactory.getLogger(getClass)

  def pca[A: Genotype](self: Data[A])(ssc: SingleStudyContext): Unit = {
    logger.info(s"perform PCA")
    val cond = LogicalParser.parse("maf >= 0.01 and maf <= 0.99 and chr != \"X\" and chr != \"Y\"")
    val common = self.variants(cond)(ssc)
    val res =  new PCA(common).pc(10)
    logger.info(s"PC dimension: ${res.rows} x ${res.cols}")
    val phenotype = Phenotype("phenotype")(ssc.sparkSession)
    val sn = phenotype.sampleNames
    val header = (1 to 10).map(i => s"_pc$i").mkString(",")
    val path = ssc.userConfig.localDir + "/output/pca.csv"
    writeDenseMatrix(path, res, Some(header))
    Phenotype.update("file://" + path, "phenotype")(ssc.sparkSession)
  }

  def titv[A: Genotype](self: Data[A])(ssc: SingleStudyContext): Unit = {
    logger.info("compute ti/tv for samples")
    val geno = implicitly[Genotype[A]]
    val cnt = self.filter(v => v.isTi || v.isTv).map{v =>
      if (v.isTi) {
        v.toCounter(g => (geno.toAAF(g)._1, 0.0), (0.0, 0.0))
      } else {
        v.toCounter(g => (0.0, geno.toAAF(g)._1), (0.0, 0.0))
      }
    }.reduce((a, b) => a ++ b)
    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    val fid = pheno.select("fid").map(_.get)
    val iid = pheno.select("iid").map(_.get)
    val outFile = "output/titv.txt"
    val pw = new PrintWriter(new File(outFile))
    pw.write("fid,iid,ti,tv\n")
    for (i <- iid.indices) {
      val c = cnt(i)
      pw.write("%s,%s,%.2f,%.2f\n" format (fid(i), iid(i), c._1, c._2))
    }
    pw.close()
  }

  def checkSex[A: Genotype](self:Data[A])(ssc: SingleStudyContext): Unit = {
    logger.info("check sex")
    val geno = implicitly[Genotype[A]]
    def toHet(g: A): (Double, Double) = {
      if (geno.isMis(g)) {
        (0, 0)
      } else if (geno.isHet(g)) {
        (1, 1)
      } else {
        (0, 1)
      }
    }
    val gb = ssc.userConfig.input.genotype.genomeBuild
    val pseudo = if (gb == GenomeBuild.hg19) {
      Hg19.pseudo
    } else {
      Hg38.pseudo
    }
    val chrX = self.filter(v =>
      (! IntervalTree.overlap(pseudo, v.toRegion)) && (v.chr == "X" | v.chr == "chrX"))
      chrX.cache()
    val chrY = self.filter(v =>
      (! IntervalTree.overlap(pseudo, v.toRegion)) && (v.chr == "Y" | v.chr == "chrY"))
      chrY.cache()
    val xHet: Option[Counter[(Double, Double)]] = if (chrX.count() == 0) {
      None
    } else {
      val res = chrX.map(v =>
        v.toCounter(toHet, (0.0, 1.0))).reduce((a, b) => a ++ b)
      Some(res)
    }
    val yCall: Option[Counter[(Double, Double)]] = if (chrY.count() == 0) {
      None
    } else {
      val res = chrY.map(v =>
        v.toCounter(geno.callRate, (0.0, 1.0))).reduce((a, b) => a ++ b)
      Some(res)
    }
    val outdir = new File(ssc.userConfig.localDir + "/output")
    outdir.mkdir()
    writeCheckSex((xHet, yCall), outdir.toString + "/checkSex.txt")(ssc)
  }

  def writeCheckSex(data: (Option[Counter[(Double, Double)]], Option[Counter[(Double, Double)]]), outFile: String)
                   (ssc: SingleStudyContext): Unit = {

    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    val fid = pheno.select("fid").map(_.get)
    //logger.info(s"fid ${fid.length}")
    val iid = pheno.select("iid").map(_.get)
    //logger.info(s"fid ${iid.length}")
    val sex = pheno.select("sex").map{
      case None => "NA"
      case Some(s) => s
    }
    //logger.info(s"sex ${sex.length}")
    val pw = new PrintWriter(new File(outFile))
    pw.write("fid,iid,sex,xHet,xHom,yCall,yMis\n")
    //logger.info(s"before counter")
    val x: Counter[(Double, Double)] = data._1.getOrElse(Counter.fill(fid.length)((0.0, 0.0)))
    val y: Counter[(Double, Double)] = data._2.getOrElse(Counter.fill(fid.length)((0.0, 0.0)))
    //logger.info(s"this should be the sample size: ${iid.length} ${x.length} ${y.length}")
    for (i <- iid.indices) {
      pw.write("%s,%s,%s,%f,%f,%f,%f\n" format (fid(i), iid(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
    }
    pw.close()
    //logger.info("are we done now?")
  }
}

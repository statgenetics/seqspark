/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.worker

import java.io.{File, PrintWriter}

import breeze.linalg.DenseVector
import breeze.stats.corrcoeff
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.IntervalTree
import org.dizhang.seqspark.assoc.Encode
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Counter, Genotype, Phenotype, Variant}
import org.dizhang.seqspark.stat.PCA
import org.dizhang.seqspark.util.Constant.{Hg19, Hg38, Pheno}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.{LogicalParser, SingleStudyContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by zhangdi on 9/20/16.
  */
object Samples {
  val logger = LoggerFactory.getLogger(getClass)

  def pca[A: Genotype](self: Data[A])(ssc: SingleStudyContext): Unit = {
    logger.info(s"perform PCA")
    val geno = implicitly[Genotype[A]]

    val cond = ssc.userConfig.qualityControl.pca.variants
    val common = self.variants(cond)(ssc)
    common.cache()
    if (ssc.userConfig.debug) {
      common.saveAsTextFile(ssc.userConfig.input.genotype.path + s".${ssc.userConfig.project}.pca")
    }
    val pruned = prune(common)(ssc)
    val res =  new PCA(pruned).pc(10)
    logger.info(s"PC dimension: ${res.rows} x ${res.cols}")
    val phenotype = Phenotype("phenotype")(ssc.sparkSession)
    val sn = phenotype.sampleNames
    val header = "iid" + Pheno.delim + (1 to 10).map(i => s"_pc$i").mkString(Pheno.delim)
    val path = ssc.userConfig.localDir + "/output/pca.csv"
    writeDenseMatrix(path, res, Some(header), Some(sn))
    Phenotype.update("file://" + path, "phenotype")(ssc.sparkSession)
  }

  def prune[A: Genotype](self: Data[A])
                        (ssc: SingleStudyContext): RDD[Array[Double]] = {
    val geno = implicitly[Genotype[A]]
    val pcaConf = ssc.userConfig.qualityControl.pca
    val coding = self.map{v =>
      val af = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce.ratio
      if (pcaConf.normalize) {
        val default = (0.0 - 2 * af)/(af * (1.0 - af))
        v.toCounter(geno.toPCA(_, af), default).toDenseVector(identity)
      } else {
        v.toCounter(geno.toBRV(_, af), 0.0).toDenseVector(identity)
      }
    }
    if (pcaConf.noprune) {
      coding.map(_.toArray)
    } else {
      val pruneConf = ssc.userConfig.qualityControl.pca.prune
      val window = pruneConf.getInt("window")
      val step = pruneConf.getInt("step")
      val r2 = pruneConf.getDouble("r2")
      coding.mapPartitions(iter =>
        prunePartition(iter, window, step, r2)
      ).map(_.toArray)
    }
  }

  def prunePartition(iv: Iterator[DenseVector[Double]],
                                  window: Int,
                                  step: Int,
                                  r2: Double): Iterator[DenseVector[Double]] = {
    var res: List[DenseVector[Double]] = Nil
    var bits = mutable.BitSet.empty
    var idx = 0
    while (iv.hasNext) {
      if (res.isEmpty) {
        res = iv.next() :: res
        bits += 0
        idx += 1
      } else {
        if (idx == window) {
          bits = bits.filter(i => i < step)
          bits = bits.map(i => i - step)
          idx = window - step
        }
        val dv = iv.next()
        val inWindow = res.take(bits.size)
        if (inWindow.exists(x => inLD(x, dv, r2))) {
          idx += 1
        } else {
          res = dv :: res
          bits += idx
          idx += 1
        }
      }
    }
    res.toIterator
  }

  def inLD(x: DenseVector[Double], y: DenseVector[Double], r2: Double): Boolean = {
    val r = corrcoeff(DenseVector.horzcat(x, y))
    math.pow(r(0,1), 2.0) >= r2
  }

  def titv[A: Genotype](self: Data[A])(ssc: SingleStudyContext): Unit = {
    logger.info("compute ti/tv for samples")
    val geno = implicitly[Genotype[A]]
    val cnt = self.filter(v => v.isTi || v.isTv).map{v =>
      if (v.isTi) {
        v.toCounter(g => (1.0, 0.0), (0.0, 0.0))
      } else {
        v.toCounter(g => (0.0, 1.0), (0.0, 0.0))
      }
    }.reduce((a, b) => a ++ b)
    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    //val fid = pheno.select("fid").map(_.get)
    val iid = pheno.select("iid").map(_.get)
    val outFile = "output/titv.txt"
    val pw = new PrintWriter(new File(outFile))
    pw.write("iid,ti,tv\n")
    for (i <- iid.indices) {
      val c = cnt(i)
      pw.write("%s,%.2f,%.2f\n" format (iid(i), c._1, c._2))
    }
    pw.close()
    logger.info("finished computing ti/tv")
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
    /**
    val gb = ssc.userConfig.input.genotype.genomeBuild
    val pseudo = if (gb == GenomeBuild.hg19) {
      Hg19.pseudo
    } else {
      Hg38.pseudo
    }
      */
    val pseudo = Hg19.pseudo
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
    //val fid = pheno.select("fid").map(_.getOrElse("NA"))
    //logger.info(s"fid ${fid.length}")
    val iid = pheno.select("iid").map(_.get)
    //logger.info(s"fid ${iid.length}")
    val sex = pheno.select("sex").map{
      case None => "NA"
      case Some(s) => s
    }
    //logger.info(s"sex ${sex.length}")
    val pw = new PrintWriter(new File(outFile))
    pw.write("iid,sex,xHet,xHom,yCall,yMis\n")
    //logger.info(s"before counter")
    val x: Counter[(Double, Double)] = data._1.getOrElse(Counter.fill(iid.length)((0.0, 0.0)))
    val y: Counter[(Double, Double)] = data._2.getOrElse(Counter.fill(iid.length)((0.0, 0.0)))
    //logger.info(s"this should be the sample size: ${iid.length} ${x.length} ${y.length}")
    for (i <- iid.indices) {
      pw.write("%s,%s,%f,%f,%f,%f\n" format (iid(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
    }
    pw.close()
    //logger.info("are we done now?")
  }
}

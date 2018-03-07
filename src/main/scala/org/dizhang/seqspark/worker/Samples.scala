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
import Variants.convertToVQC
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.ds.{Counter, Genotype, Phenotype, Variant}
import org.dizhang.seqspark.stat.PCA
import org.dizhang.seqspark.util.Constant.{Hg19, Hg38, Pheno}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.{LogicalParser, SeqContext}
import org.slf4j.LoggerFactory
import scala.collection.BitSet
import java.nio.file.Path

/**
  * Created by zhangdi on 9/20/16.
  */
object Samples {
  val logger = LoggerFactory.getLogger(getClass)

  def pca[A: Genotype](self: Data[A])(ssc: SeqContext): Unit = {
    logger.info(s"perform PCA")
    val geno = implicitly[Genotype[A]]

    val cond = ssc.userConfig.qualityControl.pca.variants
    val common = self.variants(cond, None, true)(ssc)
    common.cache()
    if (ssc.userConfig.debug) {
      common.saveAsTextFile(ssc.userConfig.input.genotype.path + s".${ssc.userConfig.project}.pca")
    }
    val pruned = prune(common)(ssc)
    val res =  new PCA(pruned).pc(10)
    if (res.rows == 0) {
      logger.warn(s"no result for PCA")
      Unit
    } else {
      logger.debug(s"PC dimension: ${res.rows} x ${res.cols}")
      val phenotype = Phenotype("phenotype")(ssc.sparkSession)
      val sn = phenotype.sampleNames
      val header = "iid" + Pheno.delim + (1 to 10).map(i => s"_pc$i").mkString(Pheno.delim)
      val path = ssc.userConfig.output.results.resolve("pca.csv")
      writeDenseMatrix(path, res, Some(header), Some(sn))
      Phenotype.update("file://" + path.toString, "phenotype")(ssc.sparkSession)
    }
  }

  def prune[A: Genotype](self: Data[A])
                        (ssc: SeqContext): RDD[Array[Double]] = {
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
    var bits = BitSet()
    var idx = 0
    while (iv.hasNext) {
      if (res.isEmpty) {
        res = iv.next() :: res
        bits += 0
        idx += 1
      } else {
        if (idx == window) {
          bits = bits.filter(i => i >= step)
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

  def titv[A: Genotype](self: Data[A])(ssc: SeqContext): Unit = {
    logger.info("compute ti/tv ratio")
    val geno = implicitly[Genotype[A]]
    val qcConf =ssc.userConfig.qualityControl
    val basis = qcConf.group.variants
    val titvBy = qcConf.titvBy.filterNot(_ == "samples")
    case class GroupKey(group: String, key: String) {
      override def toString: String = {
        s"$group.$key"
      }
    }
    val grp = titvBy.flatMap{g =>
      basis.get(g) match {
        case Some(m) => Some(g -> m)
        case None =>
          logger.warn(s"group $g not defined")
          None
      }
    }.toMap

    val grpKeys = grp.flatMap(p => p._2.keys.map(k => GroupKey(p._1, k))).toArray

    val grpLogExpr = grpKeys.map(gk => grp(gk.group)(gk.key))

    val names = grpLogExpr.flatMap(le => LogicalParser.names(le)).toSet

    val pheno = Phenotype("phenotype")(ssc.sparkSession)

    val bcBatch = ssc.sparkContext.broadcast(pheno.batch(Pheno.batch))
    val bcCtrl = ssc.sparkContext.broadcast(pheno.control)

    def cntFunc(vm: Map[String, String], ti: Boolean): A => Array[Long] = {
      g =>
        grpLogExpr.map(le => LogicalParser.eval(le)(vm)).flatMap{b =>
          if (b) {
            if (ti) {
              List(geno.toBRV(g, 0).toLong, 0L)
            } else {
              List(0L, geno.toBRV(g, 0).toLong)
            }
          } else {
            List(0L, 0L)
          }
        }
    }

    val cnt = self.filter(v => v.isTi || v.isTv).map{v =>
      val vm = v.compute(names, bcCtrl.value, bcBatch.value)
      v.toCounter(cntFunc(vm , v.isTi), Array.fill[Long](2 * grpKeys.length)(0L))
    }.reduce((a, b) => a.++(b))


    val iid = pheno.select(Pheno.iid).map(_.get)
    val outFile = ssc.userConfig.output.results.resolve("titv.txt").toAbsolutePath.toFile
    val pw = new PrintWriter(outFile)
    pw.write(s"iid,${grpKeys.flatMap(gk => List(s"${gk.toString}.ti", s"$gk.tv", s"$gk.ti/tv")).mkString(",")}\n")

    cnt.toArray.zipWithIndex.foreach{
      case (arr, i) =>
        val res = grpKeys.indices.map(j =>
          f"${arr(j * 2)}%d,${arr(j * 2 + 1)}%d,${arr(j * 2).toDouble/arr(j*2 + 1)}%.3f")
        pw.write(s"${iid(i)},${res.mkString(",")}\n")
    }

    pw.close()
    logger.info("finished computing ti/tv")
  }

  def checkSex[A: Genotype](self:Data[A])(ssc: SeqContext): Unit = {
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
    writeCheckSex((xHet, yCall), ssc.userConfig.output.results.resolve("checkSex.txt"))(ssc)
  }

  def writeCheckSex(data: (Option[Counter[(Double, Double)]], Option[Counter[(Double, Double)]]), outFile: Path)
                   (ssc: SeqContext): Unit = {

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
    val pw = new PrintWriter(outFile.toFile)
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

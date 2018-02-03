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

import java.io.PrintWriter

import breeze.stats.distributions.ChiSquared
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.ds.{Genotype, Phenotype, SparseVariant, Variant, SemiGroup}
import org.dizhang.seqspark.util.Constant.Variant._
import org.dizhang.seqspark.ds.VCF.toGeneralizedVCF
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.{LogicalParser, SeqContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions
/**
  * Created by zhangdi on 9/20/16.
  */
object Variants {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit def convertToVQC[A: Genotype](v: Variant[A]): VariantQC[A] = new VariantQC(v)

  def decompose(self: Data[String]): Data[String] = {
    logger.info("decompose multi-allelic variants")
    self.flatMap(v => decomposeVariant(v))
  }

  def titv[A](self: Data[A])(implicit ssc: SeqContext): (Int, Int) = {
    val group = ssc.userConfig.qualityControl.group.variants
    val cnt = self.map(v => if (v.isTi) (1,0) else if (v.isTv) (0, 1) else (0,0))



    if (cnt.isEmpty())
      (0, 0)
    else
      cnt.reduce((a, b) => CounterElementSemiGroup.PairInt.op(a, b))
  }

  def countByFunction[A](self: Data[A])(implicit ssc: SeqContext): Unit = {
    val cnt = self.map(v =>
      if (v.parseInfo.contains(IK.anno)) {
        worstAnnotation(v.parseInfo(IK.anno)) -> 1
      } else {
        F.Unknown -> 1
      }).countByKey()
      .toArray.sortBy(p => FM(p._1))

    val outDir = ssc.userConfig.output.results

    val pw = new PrintWriter(outDir.resolve("annotation_summary.txt").toFile)
    for ((k, v) <- cnt) {
      pw.write(s"${k.toString}: $v\n")
    }
    pw.close()
    val genes = self.flatMap(v =>
      v.parseInfo(InfoKey.anno).split(",,").map(g => g.split("::")(0))
    ).countByValue()
    val pw2 = new PrintWriter(outDir.resolve("variants_genes.txt").toFile)
    for ((k, v) <- genes) {
      pw2.write(s"$k: $v\n")
    }
    pw2.close()
  }

  def countByGroups[A](self: Data[A])
                      (implicit ssc: SeqContext): Unit = {

    val group = ssc.userConfig.qualityControl.group.variants
    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    val batch = pheno.batch(Pheno.batch)
    val controls = pheno.control
    val cnt = toGeneralizedVCF(self).countBy(group, batch, controls)(ssc.sparkContext)
    val output = ssc.userConfig.output.results.resolve("qc.txt")
    val pw = new PrintWriter(output.toFile)
    pw.append("variants:\n")
    cnt.foreach{
      case (tag, m) => m.foreach{
        case (grp, c) => pw.append(s"\t$tag.$grp:\t$c\n")
      }
    }
    pw.close()
  }


  def decomposeVariant(v: Variant[String]): Array[Variant[String]] = {
    /** decompose multi-allelic variants to bi-allelic variants */
    if (v.alleleNum == 2) {
      Array(v)
    } else {
      val alleles = v.alleles
      (1 until v.alleleNum).toArray.map{i =>
        val newV = v.map{g =>
          val s = g.split(":")
          if (s(0).contains(".")) {
            s(0)
          } else {
            val gt = s(0).split("[|/]")
            if (gt.length == 1) {
              if (gt(0) == "0") "0" else "1"
            } else {
              gt.map(j => if (j.toInt == i) "1" else "0").mkString(s(0).substring(1,2))
            }
          }
        }
        newV.meta(4) = alleles(i)
        /** trim the same bases after decomposing */
        while (newV.ref.last == newV.alt.last && newV.ref.length > 1 && newV.alt.length > 1) {
          newV.meta(3) = newV.ref.substring(0, newV.ref.length - 1)
          newV.meta(4) = newV.alt.substring(0, newV.alt.length - 1)
        }
        while (newV.ref.head == newV.alt.head && newV.ref.length > 1 && newV.alt.length > 1) {
          newV.meta(3) = newV.ref.substring(1)
          newV.meta(4) = newV.alt.substring(1)
          newV.meta(1) = s"${newV.pos.toInt + 1}"
        }

        /** parse info */
        val info = newV.parseInfo.map{
          case (k, va) =>
            val s = va.split(",")
            if (s.length == v.alleleNum - 1) {
              k -> s(i - 1)
            } else {
              k -> va
            }
        }
        newV.meta(7) = Variant.serializeInfo(info)
        newV
      }.filter{
        case SparseVariant(_,e,_,_) => e.nonEmpty
        case _ => true
      }
    }
  }

  @SerialVersionUID(100L)
  class VariantQC[A: Genotype](v: Variant[A]) extends Serializable {
    def geno = implicitly[Genotype[A]]

    /** this method compute the dictionary that
      * is needed for logical parser and counter
      * */
    def compute(names: Set[String],
                controls: Option[Array[Boolean]],
                batch: Option[Array[String]]): Map[String, String] = {
      names.map{
        case "chr" => "chr" -> v.chr
          case "pooledMaf" =>
            "pooledMaf" -> v.maf(None).toString
          case "controlsMaf" =>
            "controlsMaf" -> v.maf(controls).toString
          case "maf" =>
            val maf = v.maf(controls).toString
            "maf" -> maf
          case "informative" =>
            if (v.informative) {
              "informative" -> "1"
            } else {
              "notInformative" -> "1"
            }
          //case "batchMaf" => "batchMaf" -> v.batchMaf(ctrlInd, batch).values.min.toString
          case "missingRate" =>
            val mr = (1 - v.callRate).toString
            "missingRate" -> mr
          case "batchMissingRate" =>
            val bmr = (1 - v.batchCallRate(batch).values.min).toString
            "batchMissingRate" -> bmr
          case "alleleNum" =>
            val an = v.alleleNum.toString
            "alleleNum" -> an
          case "batchSpecific" =>
            val bs = v.batchSpecific(batch).values.max.toString
            "batchSpecific" -> bs
          case "hwePvalue" =>
            val hwe = v.hwePvalue(controls).toString
            "hwePvalue" -> hwe
          case "isFunctional" =>
            val func = v.isFunctional.toString
            "isFunctional" -> func
          case "isTi" =>
            "isTi" -> v.isTi.toString
          case "isTv" =>
            "isTv" -> v.isTv.toString
          case x =>
            val other = v.parseInfo.getOrElse(x, "0")
            x -> other
      }.toMap
    }

    def maf(controls: Option[Array[Boolean]]): Double = {
      val cache = v.parseInfo
      controls match {
        case None =>
          if (cache.contains(IK.mafAll)) {
            cache(IK.mafAll).toDouble
          } else {
            val res = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce
            v.addInfo(InfoKey.macAll,s"${res._1},${res._2}")
            v.addInfo(IK.mafAll, res.ratio.toString)
            res.ratio
          }
        case Some(indi) =>
          if (cache.contains(IK.mafCtrl)) {
            cache(IK.mafCtrl).toDouble
          } else {
            val res = v.select(indi).toCounter(geno.toAAF, (0.0, 2.0)).reduce
            v.addInfo(InfoKey.macCtrl, s"${res._1},${res._2}")
            v.addInfo(IK.mafCtrl, res.ratio.toString)
            res.ratio
          }
      }
    }
    def informative: Boolean = {
      if (v.parseInfo.contains(InfoKey.informative)) {
        v.parseInfo(InfoKey.informative) == "true"
      } else {
        val af = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce
        val res = af._1 != 0.0 && af._1 != af._2
        v.addInfo(InfoKey.informative, res.toString)
        res
      }
    }
    def batchMaf(controls: Option[Array[Boolean]],
                 batch: Option[Array[String]]): Map[String, Double] = {
      if (v.parseInfo.contains(InfoKey.batchMaf)) {
        v.parseInfo(InfoKey.batchMaf).split(",").map{kv =>
          val s = kv.split("->")
          s(0) -> s(1).toDouble
        }.toMap
      } else if (batch.isEmpty) {
        Map("all" -> maf(controls))
      } else {
        val (keyFunc, rest): (Int => String, Variant[A]) = controls match {
          case None => (i => batch.get(i), v)
          case Some(c) =>
            val b = batch.get.zip(c).filter(_._2).map(_._1)
            (i => b(i), v.select(c))
        }
        val cnt = rest.toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
        val res = cnt.map{case (k, p) => k -> p.ratio}
        val value = res.toArray.map{case (k, d) => s"$k->$d"}.mkString(",")
        v.addInfo(InfoKey.batchMaf, value)
        res
      }
    }

    def callRate: Double = {
      if (v.parseInfo.contains(InfoKey.callRate)) {
        v.parseInfo(InfoKey.callRate).toDouble
      } else {
        val cnt = v.toCounter(geno.callRate, (1.0, 1.0)).reduce
        val res = cnt.ratio
        v.addInfo(InfoKey.callRate, res.toString)
        res
      }
    }
    def batchCallRate(batch: Option[Array[String]]): Map[String, Double] = {
      if (v.parseInfo.contains(InfoKey.batchCallRate)) {
        v.parseInfo(InfoKey.batchCallRate).split(",").map{kv =>
          val s = kv.split("->")
          s(0) -> s(1).toDouble
        }.toMap
      } else if (batch.isEmpty) {
        Map("all" -> callRate)
      } else {
        val keyFunc = (i: Int) => batch.get(i)
        val cnt = v.toCounter(geno.callRate, (1.0, 1.0)).reduceByKey(keyFunc)
        val res = cnt.map{case (k, p) => k -> p.ratio}
        val value = res.toArray.map{case (k, d) => s"$k->$d"}.mkString(",")
        v.addInfo(InfoKey.batchCallRate, value)
        res
      }
    }
    def hwePvalue(controls: Option[Array[Boolean]]): Double = {
      if (v.parseInfo.contains(InfoKey.hwePvalue)) {
        v.parseInfo(InfoKey.hwePvalue).toDouble
      } else {
        val rest = controls match {
          case Some(c) => v.select(c)
          case None => v
        }
        val cnt =  rest.toCounter(geno.toHWE, (1.0, 0.0, 0.0)).reduce
        val n = cnt._1 + cnt._2 + cnt._3
        val p = (cnt._1 + cnt._2/2)/n
        val q = 1 - p
        val eAA = p.square * n
        val eAa = 2 * p * q * n
        val eaa = q.square * n
        val chisq = (cnt._1 - eAA).square/eAA + (cnt._2 - eAa).square/eAa + (cnt._3 - eaa).square/eaa
        val dis = ChiSquared(1)
        val res = 1.0 - dis.cdf(chisq)
        v.addInfo(InfoKey.hwePvalue, res.toString)
        res
      }
    }

    def batchSpecific(batch: Option[Array[String]]): Map[String, Double] = {
      if (v.parseInfo.contains(InfoKey.batchSpecific)) {
        v.parseInfo(InfoKey.batchSpecific).split(",").map{kv =>
          val s = kv.split("->")
          s(0) -> s(1).toDouble
        }.toMap
      } else if (batch.isEmpty) {
        val cnt = v.toCounter(geno.toAAF, (0.0, 2.0)).reduce
        val res = math.min(cnt._1, cnt._2 - cnt._1)
        val value = s"all->$res"
        v.addInfo(IK.batchSpecific, value)
        Map("all" -> res)
      } else {
        if (v.parseInfo.contains("dbSNP")) {
          batch.get.map(b => b -> 0.0).toMap
        } else {
          val keyFunc = (i: Int) => batch.get(i)
          val cnt = v.toCounter(geno.toAAF, (0.0, 2.0)).reduceByKey(keyFunc)
          val res = cnt.map{case (k, p) => k -> math.min(p._1, p._2 - p._1)}
          val value = res.toArray.map{case (k, c) => s"$k->$c"}.mkString(",")
          v.addInfo(InfoKey.batchSpecific, value)
          res
        }
      }
    }

    def isFunctional: Int = {
      val info = v.parseInfo
      if (info.contains(IK.anno) && FM(worstAnnotation(info(IK.anno))) < FM(F.InterGenic)) {
        val genes = parseAnnotation(v.parseInfo(IK.anno))
        if (genes.exists(p => FM(p._2) <= 4)) 1 else 0
      } else {
        0
      }
    }
  }

}

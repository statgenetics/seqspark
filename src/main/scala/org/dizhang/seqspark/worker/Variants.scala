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
import org.dizhang.seqspark.variant.VariantAnnotOp._
import org.dizhang.seqspark.ds.Phenotype.{Batch, BatchDummy, BatchImpl}
import org.dizhang.seqspark.ds.{Genotype, Phenotype, SemiGroup}
import org.dizhang.seqspark.util.Constant.Variant._
import org.dizhang.seqspark.ds.VCF.toGeneralizedVCF
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.{LogicalParser, SeqContext}
import org.dizhang.seqspark.variant._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions
/**
  * Created by zhangdi on 9/20/16.
  */
object Variants {

  val logger: Logger = LoggerFactory.getLogger(getClass)



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
      cnt.reduce((a, b) => SemiGroup.PairInt.op(a, b))
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

  def countByGroups[A: Genotype](self: Data[A])
                      (implicit ssc: SeqContext): Unit = {
    val group = ssc.userConfig.qualityControl.group.variants
    val countBy = ssc.userConfig.qualityControl.countBy
    val grp = countBy.flatMap{key =>
      group.get(key) match {
        case Some(m) => Some(key -> m)
        case None =>
          logger.warn(s"group $key not defined")
          None
      }
    }.toMap

    case class GroupKey(group: String, key: String)

    val grpKeys = grp.keys.flatMap(g => grp(g).keys.map(k => GroupKey(g, k))).toArray

    val grpLogExpr = grpKeys.map{gk =>
      grp(gk.group)(gk.key)
    }

    val pheno = Phenotype("phenotype")(ssc.sparkSession)
    val batch = pheno.batch(Pheno.batch)
    val controls = pheno.control
    val cnt = self.countBy(grpLogExpr, batch, controls)(ssc.sparkContext)
    val output = ssc.userConfig.output.results.resolve("qc.txt")
    val pw = new PrintWriter(output.toFile)
    pw.append("variants:\n")
    grpKeys.zip(cnt).foreach{
      case (GroupKey(g, k), c) =>
        pw.append(s"\t$g.$k:\t$c\n")
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



}

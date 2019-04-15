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

package org.dizhang.seqspark.meta

import breeze.linalg.diag
import breeze.linalg.{DenseMatrix => DM, DenseVector => DV}
import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.annot.{IntervalTree, RefGene}
import org.dizhang.seqspark.assoc.{SumStat, AssocMaster => AMA, AssocMethod => AME}
import org.dizhang.seqspark.assoc.SumStat.{DefaultRMWResult, RMWResult}
import org.dizhang.seqspark.ds.Variation
import org.dizhang.seqspark.ds.SummaryStatistic._
import org.dizhang.seqspark.meta.MetaMaster._
import org.dizhang.seqspark.util.Constant.{Annotation, Variant}
import org.dizhang.seqspark.util.SeqContext
import org.dizhang.seqspark.util.UserConfig.{MetaConfig, RootConfig}
import org.dizhang.seqspark.util.ConfigValue.MethodType

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
/**
  * Created by zhangdi on 6/13/16.
  */
class MetaMaster(metaContext: SeqContext) {
  val logger = LoggerFactory.getLogger(this.getClass)
  def rootConfig = metaContext.userConfig
  def sc = metaContext.sparkContext
  def metaConf = rootConfig.meta

  def run(): Unit = {
    val studies = metaConf.studyList
    val methods = metaConf.methodList
    val paths = studies.map(s =>
      metaConf.study(s).path
    ).toList
    val conditional = metaConf.conditional.map(s => Variation(s))

    /** load and merge summary statistics
      * perform conditional analysis when conditional variants are given
      * */
    val stat = mergeStatistic(paths, conditional)(sc)


    /** annotate with RefSeq */
    val annotated = annotate(stat)(rootConfig, sc)
    annotated.cache()

    /** single variant analysis */
    val snv = runSNV(annotated).collect()
    AMA.writeResults(snv,rootConfig.output.results.resolve("meta_snv").toFile)

    /** gene based analyses
      * 1. filter variants based on MAF and functional annotation
      * 2. group variants by gene
      * 3. perform meta analysis using summary statistics
      * */
    val functional = filter(annotated)
    functional.cache()

    val grouped = group(functional)

    methods.foreach{ m =>
      val mtype = metaConf.method(m).`type`
      mtype match {
        case MethodType.snv =>
          logger.warn("Single variant analysis is performed by defaut. " +
            "No need to explicitly specify it in method list")
        case MethodType.brv|MethodType.skat =>
          logger.info(s"Perform meta-analysis using method $m")
          val res = runMethod(grouped, m)(sc, metaConf).collect()
          AMA.writeResults(res, rootConfig.output.results.resolve(s"meta_$m").toFile)
        case _ => logger.warn(s"This method ($m) is not supported in meta analysis")
      }
    }
  }

}

object MetaMaster {

  type Data = RDD[RMWResult]
  type Result = RDD[(String, AME.Result)]
  val IK = Variant.InfoKey
  val F = Annotation.Feature
  val FM = F.values.zipWithIndex.toMap

  /** recursively load summary statistics and merge to results */
  def mergeStatistic(paths: List[String], conditionals: Array[Variation])
                    (implicit sc: SparkContext): Data = {
    @tailrec
    def f(rest: List[String], acc: Data): Data = {
      if (rest.isEmpty)
        acc
      else {
        val next = loadSummaryStatistic(rest.head, conditionals)
        f(rest.tail, acc.add(next).statistic)
      }
    }
    f(paths.tail, loadSummaryStatistic(paths.head, conditionals))
  }

  def loadSummaryStatistic(path: String,
                           conditionals: Array[Variation])
                          (implicit sc: SparkContext): Data = {

    sc.objectFile[SumStat.DefaultRMWResult](path).map(r =>
      r.conditional(conditionals)
    )
  }

  def runSNV(data: Data): Result = {
    data.flatMap{r =>
      val res = r.score /:/ diag(r.variance).map(math.sqrt)
      r.vars.zip(res.toArray).map{
        case (v, s) =>
          val key = v.toRegion
          val dis = new Gaussian(0.0, 1.0)
          val p = (1.0 - dis.cdf(s)) * 2
          val value = AME.BurdenAnalytic(Array(v), s, Some(p), v.info.getOrElse("None"))
          (key, value)
      }
    }
  }

  def runMethod(data: RDD[(String, RMWResult)],
                method: String)
               (implicit sc: SparkContext,
                conf: MetaConfig): Result = {
    val methodConf = conf.method(method)
    val methodType = methodConf.`type`
    val weightType = methodConf.weight
    data.map{
      case (g, r) =>
        methodType match {
          case MethodType.skato =>
            val m = methodConf.misc.method
            val rhos = methodConf.misc.rhos
            g -> MetaMethod.SKATO(r, weightType, m, rhos).result

          case MethodType.skat =>
            val m = methodConf.misc.method
            val rho = methodConf.misc.rho
            g -> MetaMethod.SKAT(r, weightType, m, rho).result
          case _ =>
            g -> MetaMethod.Burden(r, weightType).result
        }
    }
  }

  /** filter variants based on MAF and functional annotation
    * */
  def filter(data: Data): Data = {
    data.map{r =>
      /** choose rare variants */
      val rare = r.vars.zipWithIndex.filter{
        case (v, i) =>
          val mac = v.parseInfo(IK.mac).split(",")
          val maf = mac(0).toDouble/mac(1).toDouble
          maf < 0.05 || maf > 0.95
      }
      /** choose functional variants */
      val functional = rare.filter{
        case (v, i) =>
          val info = v.parseInfo
          if (info.contains(IK.anno) && FM(worstAnnotation(info(IK.anno))) < FM(F.InterGenic)) {
            val genes = parseAnnotation(info(IK.anno))
            genes.exists(p => FM(p._2) <= 4)
          } else {
            false
          }
      }
      val funcVars = functional.map(_._1)
      val funcIdx = functional.map(_._2)
      /** choose target score values */
      val funcScore = DV(for (i <- funcIdx) yield r.score(i))
      /** choose target variance values */
      val tmp = for {
        i <- funcIdx
        j <- funcIdx
      } yield r.variance(i,j)
      val funcVariance = new DM[Double](funcIdx.length, funcIdx.length, tmp)
      DefaultRMWResult(r.binary, r.sampleSize, funcVars, funcScore, funcVariance)
    }
  }

  /** group variants by gene */
  def group(data: Data): RDD[(String, RMWResult)] = {
    /** group the variant idx by gene, duplicate when necessary */
    data.flatMap{r =>
      val geneIdx: Array[(String, Array[Int])] =
        r.vars.zipWithIndex.flatMap{
          case (v,i) => /** var and idx */
            val anno = v.parseInfo(IK.anno)
            val genes =
             if (FM(worstAnnotation(anno)) >= FM(F.InterGenic))
               Array[(String, F.Value)]()
             else
               parseAnnotation(v.parseInfo(IK.anno))
            /** choose genes in which this variant is functional */
            genes.filter(p => FM(p._2) <= 4).map(p => p._1 -> i)
        }.groupBy(_._1).map{
          case (g, idx) =>
            g -> idx.map(_._2).sorted /** sort the idx in each gene */
        }.toArray
      val geneVars = for (g <- geneIdx) yield g._1 -> g._2.map(j => r.vars(j).copy())
      val geneScore = for (g <- geneIdx) yield DV(g._2.map(j => r.score(j)))
      val geneVariance = for (g <- geneIdx) yield {
        val tmp = for {m <- g._2; n <- g._2} yield r.variance(m, n)
        new DM(g._2.length, g._2.length, tmp)
      }
      geneVars.zip(geneScore).zip(geneVariance).map{
        case (((g, vars), s), variance) =>
          g -> DefaultRMWResult(r.binary, r.sampleSize, vars, s, variance)
      }
    }
  }

  /** functional annotation */
  def annotate(data: Data)(conf: RootConfig, sc: SparkContext): Data = {
    val dbConf = conf.annotation.RefSeq
    val build = "hg19"
    val coordFile = dbConf.getString("coord")
    val seqFile = dbConf.getString("seq")
    val refGene = sc.broadcast(RefGene(build, coordFile, seqFile)(sc))
    data.map{r =>
      r.vars.foreach{v =>
          val annot = IntervalTree.lookup(refGene.value.loci, v).filter(l =>
            refGene.value.seq.contains(l.mRNAName)).map { l =>
            (l.geneName, l.mRNAName, l.annotate(v, refGene.value.seq(l.mRNAName)))
          }
          annot match {
            case Nil =>
              v.addInfo(IK.anno, F.InterGenic.toString)
            case _ =>
              val merge = annot.map(p => (p._1, s"${p._2}:${p._3.toString}")).groupBy(_._1)
                .map{case (k, value) => "%s::%s".format(k, value.map(x => x._2).mkString(","))}
                .mkString(",,")
              v.addInfo(IK.anno, merge)
          }
      }
    }
    data
  }
}
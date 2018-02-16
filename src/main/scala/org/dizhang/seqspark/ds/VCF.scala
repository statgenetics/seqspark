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

package org.dizhang.seqspark.ds

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.annot.Regions
import org.dizhang.seqspark.ds.Phenotype.Batch
import org.dizhang.seqspark.util.UserConfig.MethodConfig
import org.dizhang.seqspark.util.{LogicalParser, SeqContext, ConfigValue => CV}
import org.dizhang.seqspark.worker.Variants._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

/**
  * generalize VCF format
  */

@SerialVersionUID(102L)
class VCF[A: Genotype](self: RDD[Variant[A]]) extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def regions(regions: Regions)(sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(regions)
    self.filter(v => bc.value.overlap(v.toRegion))
  }
  def samples(indicator: Array[Boolean])(sc: SparkContext): RDD[Variant[A]] = {
    val bc = sc.broadcast(indicator)
    self.map(v => v.select(bc.value))
  }

  /** select samples by all/none or phenotype name */
  def samples(sampleExpr: CV.Samples)
             (implicit ssc: SeqContext): RDD[Variant[A]] = {
    sampleExpr match {
      case CV.Samples.all => self
      case CV.Samples.none => self.map(_.toDummy)
      case CV.Samples.by(col) =>
        val pheno = Phenotype("phenotype")(ssc.sparkSession)
        val select = pheno.indicate(col)
        self.map(v => v.select(select))
    }
  }

  def toDummy: RDD[Variant[A]] = {
    self.map(v => v.toDummy)
  }

  def groupByVarNum: RDD[(String, Iterable[Variant[A]])] = {
    val sampleSize = self.first().length
    val varNum = if (sampleSize == 0) 1000 else 1000000/sampleSize //the matrix should only have 1000000 elements
    val binSize = self.mapPartitions(i => Array(i.size).toIterator).max/varNum + 1
    val pars = self.partitions.length
    self.zipWithUniqueId().map{
      case (v, idx) =>
        val k = idx%pars
        val i = idx/pars/varNum
        (k * binSize + i).toString -> v
    }.groupByKey()
  }

  /** the input groups
    * Map[label, Map[grp, condition] ]
    *
    * */
  def countBy(exprs: Array[LogicalParser.LogExpr],
              batch: Batch,
              controls: Option[Array[Boolean]])
             (implicit sc: SparkContext): Array[Int] = {

    def merge(m1: Map[String, Int], m2: Map[String, Int]): Map[String, Int] = {
      m1 ++ (for ((k, n) <- m2) yield if (m1.contains(k)) k -> (n + m1(k)) else k -> n)
    }

    val bcBatch = sc.broadcast(batch)
    val bcControls = sc.broadcast(controls)
    val names = exprs.flatMap(x => LogicalParser.names(x)).toSet
    self.map{v =>
      val vm = v.compute(names, bcControls.value, bcBatch.value)
      exprs.map(e => LogicalParser.eval(e)(vm)).map(b => if (b) 1 else 0)
    }.reduce{(a, b) =>
      SemiGroup.Ints.op(a, b)
    }
  }

  /**
  def withGroup(cond: Array[LogicalParser.LogExpr],
                batch: Option[Array[String]],
                controls: Option[Array[Boolean]])
               (implicit sc: SparkContext): RDD[(Int, Variant[A])] = {
    val bcBatch = sc.broadcast(batch)
    val bcControls = sc.broadcast(controls)
    self.map{v =>
      cond.map(c => )
    }
  }
  */

  def variants(cond: LogicalParser.LogExpr,
               updateInfo: Option[String] = None,
               filter: Boolean = true)(ssc: SeqContext): RDD[Variant[A]] = {
    if (cond == LogicalParser.T) {
      logger.info("condition empty, no need to filter variants")
      updateInfo.foreach{key =>
        self.map(v => v.addInfo(key))
      }
      self
    } else {
      val conf = ssc.userConfig
      val pheno = Phenotype("phenotype")(ssc.sparkSession)
      val batch = pheno.batch(conf.input.phenotype.batch)
      val controls = pheno.select("control")
      val ctrlInd = if (controls.forall(c => c.isEmpty || c.get == "1")) {
        /** if everybody is either control or unknown, assume they are all controls */
        None
      } else {
        Some(controls.map{case Some("1") => true; case _ => false})
      }

      val myCond = LogicalParser.view(cond)
      logger.info(s"filter variants with [$myCond] ...")
      val names: Set[String] = LogicalParser.names(cond)

      self.filter{v =>
        val varMap = names.toArray.map{
          case "chr" => "chr" -> v.chr
          case "pooledMaf" =>
            "pooledMaf" -> v.maf(None).toString
          case "controlsMaf" =>
            "controlsMaf" -> v.maf(ctrlInd).toString
          case "maf" =>
            val maf = v.maf(ctrlInd).toString
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
            val hwe = v.hwePvalue(ctrlInd).toString
            "hwePvalue" -> hwe
          case "isFunctional" =>
            val func = v.isFunctional.toString
            "isFunctional" -> func
          case x =>
            val other = v.parseInfo.getOrElse(x, "0")
            x -> other
        }.toMap

        lazy val pass = LogicalParser.eval(cond)(varMap)
        updateInfo.foreach(k => if (pass) v.addInfo(k))
        !filter || pass
      }
    }
  }
}

object VCF {

  implicit def toGeneralizedVCF[A: Genotype](data: RDD[Variant[A]]): VCF[A] = new VCF(data)

  case class Format[A](name: String, num: Int, sep: String)

  /**
  case class

  class header(version: String,
               filter: Set[String],
               format: Map[String, Format[_]],


              ) {

  }
  */
}

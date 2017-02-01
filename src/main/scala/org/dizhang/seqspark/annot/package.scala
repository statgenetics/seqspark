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

package org.dizhang.seqspark

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.util.LogicalParser
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.worker.Data
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by zhangdi on 8/15/16.
  */
package object annot {
  val logger = LoggerFactory.getLogger(getClass)


  def getDBs(conf: RootConfig): Map[String, Set[String]] = {
    val qcDBs = getDBs(conf.qualityControl.variants)
    val annDBs = getDBs(conf.annotation.variants)
    //logger.info(s"qcDBs: ${qcDBs.keys.mkString(",")}")
    //logger.info(s"annDBs: ${annDBs.keys.mkString(",")}")

    if (conf.pipeline.last == "association") {
      val assDBs = conf.association.methodList.map{ m =>
        val cond = LogicalParser.parse(conf.association.method(m).misc.variants)
        getDBs(cond)
      }.toList
      (annDBs :: qcDBs :: assDBs).reduce((a, b) => addMap(a, b))
    } else {
      addMap(annDBs, qcDBs)
    }
  }

  def addMap(x1: Map[String, Set[String]], x2: Map[String, Set[String]]) : Map[String, Set[String]] = {
    x1 ++ (for ((k, v) <- x2) yield if (x1.contains(k)) k -> (x1(k) ++ v) else k -> v)
  }

  def getDBs(cond: LogicalParser.LogExpr): Map[String, Set[String]] = {
    LogicalParser.names(cond).filter(n => n.contains(".")).map{n =>
      val s = n.split("\\.")
      (s(0), s(1))
    }.groupBy(_._1).map{
      case (k, v) => k -> v.map(_._2).toSet
    }
  }

  def getDBs(conf: Config): Map[String, Set[String]] = {
    conf.getObject("addInfo").map{
      case (k, v) =>
        val vs = v.toString.split("/").map(_.replaceAll(" ", ""))
            .map(_.split("."))
        k -> vs.filter(_.length > 1).map(_(1)).toSet
    }.toMap
  }

  def linkGeneDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link gene database ...")
    val dbConf = conf.annotation.RefSeq
    val build = "hg19"
    val coordFile = dbConf.getString("coord")
    val seqFile = dbConf.getString("seq")
    val refSeq = sc.broadcast(RefGene(build, coordFile, seqFile)(sc))
    input.map(v => v.annotateByVariant(refSeq))
  }

  def linkVariantDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {

    val dbTerms = getDBs(conf)
    if (dbTerms.isEmpty) {
      logger.info("no need to join variant databases")
      input
    } else {
      logger.info("link variant database ...")
      logger.info(s"dbs: ${dbTerms.keys.mkString(",")}")
      var paired = input.map(v => (v.toVariation(), v))
      dbTerms.foreach{
        case (k, v) =>
          logger.info(s"join database ${k} with fields ${if (v.isEmpty) "None" else v.mkString(",")}")
          val db = VariantDB(conf.annotation.config.getConfig(k), v, conf.partitions)(sc)

          paired = paired.leftOuterJoin(db.info).map{
            case (va, (vt, Some(info))) =>
              vt.addInfo(k)
              db.header.zip(info).foreach{
                case (ik, iv) => vt.addInfo(ik, iv)}
              va -> vt
            case (va, (vt, None)) => va -> vt
          }
        //val cnt = paired.map{_.2}.reduce(_ + _)
        //logger.info(s"database $k annotated: $cnt")
      }
      paired.map(_._2)
    }

  }
}

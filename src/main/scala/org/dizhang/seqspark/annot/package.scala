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


}

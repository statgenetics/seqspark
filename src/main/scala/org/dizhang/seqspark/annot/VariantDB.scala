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

package org.dizhang.seqspark.annot

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Region, Variant, Variation}
import org.slf4j.{Logger, LoggerFactory}
import org.dizhang.seqspark.util.UserConfig.DatabaseConfig
import org.dizhang.seqspark.worker.Variants
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.ConfigValue.DBFormat

import scala.collection.JavaConverters._
/**
  * Created by zhangdi on 8/17/16.
  */

trait VariantDB {
  val header: Array[String]
  val info: RDD[(Variation, Array[String])]
}

object VariantDB {

  val dbExists = Constant.Variant.dbExists

  val logger: Logger = LoggerFactory.getLogger(getClass)

  case class Default(header: Array[String], info: RDD[(Variation, Array[String])]) extends VariantDB

  def apply(conf: DatabaseConfig, fields: Set[String], jobs: Int)(sc: SparkContext): VariantDB = {
    val data = sc.textFile(conf.path).cache()
    val header = fields.toArray
    logger.info(s"build Variant database from ${conf.path}")
    val info =
    conf.format match {
      case DBFormat.vcf =>
        fromVCF(conf.path, header, jobs)(sc)
      case _ =>
        fromPlainFile(
          conf.path,
          conf.delimiter,
          header,
          conf.header,
          jobs)(sc)
    }
    Default(header, info)
  }

  def fromVCF(path: String, header: Array[String], jobs: Int)(sc: SparkContext): RDD[(Variation, Array[String])] = {
    sc.textFile(path, jobs).filter(! _.startsWith("#")).flatMap{l =>
      val vs = Variants.decomposeVariant(Variant.fromString(l, "0/0", noSample = true))
      vs.map{v =>
        val i = v.parseInfo
        v.toVariation() -> header.map(f =>
          if (f == dbExists)
            "true"
          else
            i.getOrElse(f, ""))
      }
    }
  }
  def fromPlainFile(path: String,
                    delim: String,
                    targetHeader: Array[String],
                    sourceHeader: Array[String],
                    jobs: Int)
                   (sc: SparkContext): RDD[(Variation, Array[String])] = {
    sc.textFile(path, jobs).flatMap{l =>
      val s = l.split(delim)
      val rec = sourceHeader.zip(s).toMap
      val region = Region(rec("chr"), rec("pos").toInt)
      val alts = rec("alt").split(",")
      alts.indices.map{i =>
        var r = rec("ref")
        var a = alts(i)
        var s = region.start
        while (r.last == a.last && r.length > 1 && a.length > 1) {
          r = r.substring(0, r.length - 1)
          a = a.substring(0, a.length - 1)
        }
        while (r.head == a. head && r.length > 1 && a.length > 1) {
          r = r.substring(1)
          a = a.substring(1)
          s += 1
        }
        val variation = Variation(region.chr, s, r, a, None)
        variation -> targetHeader.map{f =>
          if (f == dbExists)
            "true"
          else {
            val vs = rec(f).split(",")
            if (vs.length == alts.length) {
              vs(i)
            } else {
              rec(f)
            }
          }
        }
      }

    }

  }

}

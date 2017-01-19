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
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Region, Variant, Variation}

import scala.collection.JavaConverters._
/**
  * Created by zhangdi on 8/17/16.
  */

trait VariantDB {
  val header: Array[String]
  val info: RDD[(Variation, Array[String])]
}

object VariantDB {

  case class Default(header: Array[String], info: RDD[(Variation, Array[String])]) extends VariantDB

  def apply(conf: Config, fields: Set[String], jobs: Int)(sc: SparkContext): VariantDB = {
    val data = sc.textFile(conf.getString("path")).cache()
    val header = fields.toArray
    if (conf.getString("format") == "vcf") {
      logger.info(s"build Variant database from ${conf.getString("path")}")
      Default(header, fromVCF(conf.getString("path"), header, jobs)(sc))
    } else {
      val info = fromPlainFile(
        conf.getString("path"),
        conf.getString("delimiter"),
        header,
        conf.getStringList("header").asScala.toArray,
        jobs)(sc)
      Default(header, info)
    }
  }

  def fromVCF(path: String, header: Array[String], jobs: Int)(sc: SparkContext): RDD[(Variation, Array[String])] = {
    sc.textFile(path, jobs).filter(! _.startsWith("#")).map{l =>
      val v = Variant.fromString(l, "0/0", noSample = true)
      val info = v.parseInfo
      v.toVariation() -> header.map(f => info(f))
    }
  }
  def fromPlainFile(path: String,
                    delim: String,
                    targetHeader: Array[String],
                    sourceHeader: Array[String],
                    jobs: Int)
                   (sc: SparkContext): RDD[(Variation, Array[String])] = {
    sc.textFile(path, jobs).map{l =>
      val s = l.split(delim)
      val rec = sourceHeader.zip(s).toMap
      val region = Region(rec("chr"), rec("pos").toInt)
      val variation = Variation(region.chr, region.start, region.end, rec("ref"), rec("alt"), None)
      variation -> targetHeader.map(f => rec(f))
    }

  }

}

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
import org.dizhang.seqspark.annot.dbNSFP.Record
import org.dizhang.seqspark.ds.{Region, Variation}
import org.dizhang.seqspark.util.ConfigValue.GenomeBuild

/**
  * Created by zhangdi on 4/13/16.
  */
class dbNSFP(val data: RDD[Record]) {
  def zipWithKey(build: GenomeBuild.Value): RDD[(Variation, Record)] = {
    data.map(r => (r.toVariantion(build), r))
  }
}

object dbNSFP {

  var header: Array[String] = Array[String]()

  def apply(path: String)(implicit sc: SparkContext): dbNSFP = {
    val raw = sc.textFile(path)
    raw.cache()
    header = raw.first().split("\t")
    val rest = raw.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    new dbNSFP(rest.map(l => new Record(l.split("\t"))))
  }

  class Record(val data: Array[String]) {
    def apply(key: String): String = {
      header.zip(data).toMap.apply(key)
    }

    def foreach(f: (String, String) => Unit): Unit = {
      header.zip(data).foreach(p => f(p._1, p._2))
    }

    def toVariantion(build: GenomeBuild.Value): Variation = {
      val r: Region = build match {
        case GenomeBuild.hg38 =>
          val chr = this("chr")
          val pos = this("pos").toInt - 1
          Region(chr, pos)
        case GenomeBuild.hg19 =>
          val chr = this("hg19_chr")
          val pos = this("hg19_pos").toInt - 1
          Region(chr, pos)
        case _ =>
          val chr = this("hg18_chr")
          val pos = this("hg18_pos").toInt - 1
          Region(chr, pos)
      }
      new Variation(r, this("ref"), this("alt"))
    }
  }

}
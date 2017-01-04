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

import org.dizhang.seqspark.ds._
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}
import scala.io.Source

/**
  * Created by zhangdi on 9/28/16.
  */

class Chain(val map: Map[String, IntervalTree[Named]]) {
  val logger = LoggerFactory.getLogger(this.getClass)
  def liftOver(chr: String, start: Int, end: Int): (String, Int, Int) = {
    if (! map.contains(chr)) {
      logger.warn(s"cannot find $chr in chain file")
      (chr, start, end)
    } else {
      val query = Region("0", start, end)
      val target = IntervalTree.lookup(map(chr), query).head
      val sStart = target.start
      val sEnd = target.end
      val regex = """[+-],(\w+):(\d+)-(\d+)""".r
      target.name match {
        case regex(tStrand, tChr, tStart, tEnd) =>
          val real = query.intersect(target)
          val offset = math.abs(real.start - sStart)
          val size  = real.length
          val iStart = if (tStrand == "+") {
            tStart.toInt + offset
          } else {
            tEnd.toInt - offset - size
          }
          (tChr, iStart, iStart + size)
      }
    }
  }
}

object Chain {
  val logger = LoggerFactory.getLogger(this.getClass)

  def hg19Tohg38: Map[String, IntervalTree[Named]] = {
    val instream = getClass.getResourceAsStream("hg19ToHg38.over.chain")
    val raw = Source.fromInputStream(instream).getLines().toArray.filterNot(_ == "")
    //val headers = raw.filter(_.startsWith("chain")).map(_.split("\\s+"))
    build(raw)
  }

  def build(data: Array[String]): Map[String, IntervalTree[Named]] = {
    val res = MMap[String, IntervalTree[Named]]()
    var cur: Head = new Head(Array.empty[String])
    data.foreach{l =>
      val s = l.split("\\s+")
      if (s(0) == "chain" && List(12,13).contains(s.length)) {
        cur = new Head(s)
        if (! res.contains(cur.sourceName)) {
          res(cur.sourceName) = Leaf[Named]()
        }
      } else if (s(0) != "chain" && s.length == 3) {
        val size = s(0).toInt
        val sgap = s(1).toInt
        val tgap = s(2).toInt
        if (cur.targetStrand == "+") {
          val target = s"${cur.targetStrand},${cur.targetName}:${cur.tfrom}-${cur.tfrom + size}"
          val region = Named(0, cur.sfrom, cur.sfrom + size, target)
          res(cur.sourceName) = IntervalTree.insert(res(cur.sourceName), region)
        } else {
          val taget = s"${cur.targetStrand},${cur.targetName}:${cur.targetSize - cur.tfrom - size}" +
            s"${cur.targetSize - cur.tfrom}"
          val region = Named(0, cur.sfrom, cur.sfrom + size, taget)
        }
        cur.sfrom += size + sgap
        cur.tfrom += size + tgap
      } else if (s(0) != "chain" && s.length == 1) {
        val size = s(0).toInt
        if (cur.targetStrand == "+") {
          val target = s"${cur.targetStrand},${cur.targetName}:${cur.tfrom}-${cur.tfrom + size}"
          val region = Named(0, cur.sfrom, cur.sfrom + size, target)
          res(cur.sourceName) = IntervalTree.insert(res(cur.sourceName), region)
        } else {
          val taget = s"${cur.targetStrand},${cur.targetName}:${cur.targetSize - cur.tfrom - size}" +
            s"${cur.targetSize - cur.tfrom}"
          val region = Named(0, cur.sfrom, cur.sfrom + size, taget)
        }
      } else {
        logger.error(s"invalid chain format: $l")
      }
    }
    res.toMap
  }


  private class Head(line: Array[String]) {
    def score = line(1)
    def sourceName = line(2).replaceFirst("chr", "")
    def sourceSize = line(3).toInt
    def sourceStrand = line(4)
    def sourceStart = line(5).toInt
    def sourceEnd = line(6).toInt
    def targetName = line(7).replaceFirst("chr", "")
    def targetSize = line(8).toInt
    def targetStrand = line(9)
    def targetStart = line(10).toInt
    def targetEnd = line(11).toInt
    var sfrom = sourceStart
    var tfrom = targetStart
  }

}
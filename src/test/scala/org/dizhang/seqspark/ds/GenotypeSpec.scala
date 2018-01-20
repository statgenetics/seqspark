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

import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 1/4/17.
  */
class GenotypeSpec extends FlatSpec with Matchers {
  val logger = LoggerFactory.getLogger(getClass)

  val raw = {
    Array(
      ".:0",
      "0:4",
      "1:3",
      "./.:2",
      "0/0:12",
      "0/1:2",
      "1/0:1",
      "1/1:0",
      ".|.:1",
      "0|0:8",
      "0|1:7",
      "1|0:9",
      "1|1:3"
    )
  }

  val simple = raw.map(g => Genotype.Raw.toSimpleGenotype(g))


  "A Raw Genotype" should "be able to convert to simple and back" in {
    val s = raw.map(g => Genotype.Raw.toSimpleGenotype(g))
    logger.debug(s"raw to simple: ${s.mkString(",")}")
    val r = s.map(g => Genotype.Simple.toVCF(g))
    logger.debug(s"simple to raw: ${r.mkString(",")}")
    r.map(g => Genotype.Raw.toSimpleGenotype(g)) should be (s)
  }

  "A Raw Genotype" should "give right callRate" in {
    val c = raw.map(g => Genotype.Raw.callRate(g))
    logger.debug(s"raw callrate: ${c.mkString(",")}")
    val cnt = Counter.fromIndexedSeq(c, (1.0, 1.0)).reduce
    logger.debug(s"raw callrate: ${cnt._1/cnt._2}")
  }

  "A Simple Genotype" should "give right callRate" in {
    val c = simple.map(g => Genotype.Simple.callRate(g))
    logger.debug(s"simple callRate: ${c.mkString(",")}")
    val cnt = Counter.fromIndexedSeq(c, (1.0, 1.0)).reduce
    logger.debug(s"simple callrate: ${cnt._1/cnt._2}")
  }

  "A Raw Genotype" should "give right MAF" in {
    val maf = raw.map(g => Genotype.Raw.toAAF(g))
    logger.debug(s"raw maf: ${maf.mkString(",")}")
    val cnt = Counter.fromIndexedSeq(maf, (0.0, 2.0)).reduce
    logger.debug(s"raw maf: ${cnt._1/cnt._2}")
  }

  "A Simple Genotype" should "give right MAF" in {
    val maf = simple.map(g => Genotype.Simple.toAAF(g))
    logger.debug(s"simple maf: ${maf.mkString(",")}")
    val cnt = Counter.fromIndexedSeq(maf, (0.0, 2.0)).reduce
    logger.debug(s"simple maf: ${cnt._1/cnt._2}")
  }

}

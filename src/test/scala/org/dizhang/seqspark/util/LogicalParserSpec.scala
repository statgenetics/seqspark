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

package org.dizhang.seqspark.util

import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

/**
  * Created by zhangdi on 12/3/16.
  */
class LogicalParserSpec extends FlatSpec with Matchers {

  val logger = LoggerFactory.getLogger(getClass)

  "A LogicalParser" should "be able to constructed" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38")
    LogicalParser.parse(List("maf < 0.01 or maf > 0.99", "SS_PASS"))
    LogicalParser.parse(List("maf >= 0.01", "maf <= 0.99", "SS_PASS"))
  }

  "A LogicalParser" should "eval to true" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38")
    LogicalParser.eval(lp)(Map("INFO.AN"->"3900", "INFO.AC"->"40")) should be (true)
  }

  "A LogicalParser" should "eval to false" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38 and INFO.AC<3750")
    LogicalParser.eval(lp)(Map("INFO.AN"->"3900", "INFO.AC"->"3775")) should be (false)
  }

  "A LogicalParser" should "handle String comparisons" in {
    val lp = LogicalParser.parse("chr != \"X\" and chr != \"Y\"")
    LogicalParser.eval(lp)(Map("chr" -> "11")) should be (true)
  }

  "A LogicalParser" should "handle nested conditions" in {
    val lp = LogicalParser.parse(List("missingRate < 0.1", "batchMissingRate < 0.1", "hwePvalue >= 1e-5"))
    logger.debug(LogicalParser.view(lp))

    LogicalParser.eval(lp)(
      Map("missingRate" -> "0.3", "batchMissingRate" -> "0.4", "hwePvalue" -> "0.001")
    ) should be (false)
  }

  "A LogicalParser" should "parse filter" in {
    val lp = LogicalParser.parse(List("FILTER==\"PASS\"", "INFO.AN>=3468", "INFO.AC>=34", "INFO.AC<=3815"))
    logger.debug(LogicalParser.view(lp))
  }
}

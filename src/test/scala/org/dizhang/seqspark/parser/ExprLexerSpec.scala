/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.seqspark.parser

import org.dizhang.seqspark.BaseSpecs

class ExprLexerSpec extends BaseSpecs.UnitSpec {

  val default: ExprLexer = ExprLexer()

  def show(code: String): Unit = {
    logger.info(s"${default(code)}")
  }

  "A ExprLexer" should "parse logical expressions" in {
    val code = "maf < 0.4 && INFO.AN == 3 or log(maf) > 4"

    logger.info(default(code).toString)
  }

  it should "parse string literal in single quotes" in {
    val code = "x == 'x-man' and y == 'batman'"

    logger.info(default(code).toString)
  }
  it should "parse numbers" in {
    val code = "maf() > 0.54 min(maf(batch)) <= .5 0.5 -8 -90. +.78E4 +1.e-2 3.8E+4"

    logger.info(default(code).toString)
    show("min(hwePvalue(batch)) >= 1e-6")
    show("x > 1 || y == '2' and (z ne 7.5 || j)")
    show("log(maf())/7.5")
  }
}

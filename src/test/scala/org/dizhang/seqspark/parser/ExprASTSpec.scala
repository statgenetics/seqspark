/*
 * Copyright 2018 Zhang Di
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

package org.dizhang.seqspark.parser
import org.dizhang.seqspark.BaseSpecs.UnitSpec
class ExprASTSpec extends UnitSpec {

  def show(code: String): Unit = {
    logger.info(s"${ExprAST.compile(code)}")
  }

  "A ExprAST" should "parse simple logical expressions" in {
    show("maf() >= 0.01")
    show("min(missingRate(batch)) < 0.1")
    show("min(hwePvalue(batch)) >= 1e-6")
    show("x > 1 || y == '2' and (z ne 7.5 || j)")
  }

  it should "parse compound logical expressions" in {
    show("! maf or ! maf() and ! maf(b)")
  }

  it should "parse simple arithmetic expressions" in {
    show("log(maf())/7.5")
    show("min(maf(batch)) + 2.4 + -min(x)")
  }

  it should "catch type errors" in {
    show("5 and true")
  }
}

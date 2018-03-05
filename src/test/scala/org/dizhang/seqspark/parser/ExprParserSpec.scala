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
import org.dizhang.seqspark.BaseSpecs
import ExprToken._

class ExprParserSpec extends BaseSpecs.UnitSpec {
  val default: ExprParser = ExprParser()

  val res = List[ExprToken](Identifier("maf"), LeftParen, RightParen, COMP(">="), DoubleLit(1e-6))

  "A ExprParser" should "parse simple logical expressions" in {
    logger.info(s"${default(res)}")
  }

}

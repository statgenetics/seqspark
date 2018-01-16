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

package org.dizhang.seqspark.util

import org.dizhang.seqspark.BaseSpecs

class QueryParserSpec extends BaseSpecs.UnitSpec {

  def msg(expr: String): List[String] = {
    val qe = QueryParser.parse(expr)
    QueryParser.dbKeys(qe).map(p => s"db: ${p._1} keys: ${p._2.mkString(",")}").toList
  }

  "A QueryParser" should "parse simple db existance" in {
    val db = "dbSNP"

    msg(db).foreach(m => logger.info(m))

  }

  it should "parse db and key" in {

    val dbKey = "gnomAD.AN"

    msg(dbKey).foreach(m => logger.info(m))

  }

}

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
import org.dizhang.seqspark.BaseSpecs.UnitSpec
import org.dizhang.seqspark.SeqSpark

class UserConfigSpec extends UnitSpec {

  val confFile = getClass.getResource("/qc.conf").getPath
  val conf = SeqSpark.readConf(confFile)

  "A GroupConfig" should "parse group info" in {
    logger.info(s"${conf.qualityControl.group.variants}")
  }
}

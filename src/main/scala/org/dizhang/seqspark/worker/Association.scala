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

package org.dizhang.seqspark.worker
import org.dizhang.seqspark.assoc.AssocMaster
import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.util.SeqContext
import org.slf4j.LoggerFactory

object Association {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply[B: Genotype](input: Data[B])(implicit ssc: SeqContext): Unit = {
    if (ssc.userConfig.pipeline.contains("association"))
      if (input.isEmpty()) {
        logger.warn(s"no variants left. cannot perform association analysis")
      } else {
        new AssocMaster(input).run()
      }
  }
}

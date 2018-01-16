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

import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.util.SeqContext

object Export {

  def apply[A: Genotype](data: Data[A])(implicit ssc: SeqContext): Unit = {
    val geno = implicitly[Genotype[A]]
    val conf = ssc.userConfig.output.genotype
    if (conf.export) {
      val path = if (conf.path.isEmpty) ssc.userConfig.input.genotype.path + ".export" else conf.path
      data.map(v => v.toString).saveAsTextFile(path)
    }
    if (conf.save || conf.cache) {
      data.saveAsObjectFile(conf.path)
    }
  }
}

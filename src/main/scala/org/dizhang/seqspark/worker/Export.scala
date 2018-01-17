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

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.ds.VCF._
import org.dizhang.seqspark.util.SeqContext
import org.dizhang.seqspark.util.UserConfig.hdfs
import org.apache.hadoop
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
object Export {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply[A: Genotype](data: Data[A])(implicit ssc: SeqContext): Unit = {
    val geno = implicitly[Genotype[A]]
    val conf = ssc.userConfig.output.genotype
    if (conf.export) {
      val path = if (conf.path.isEmpty)
        ssc.userConfig.input.genotype.path + "." + ssc.userConfig.project
      else
        conf.path
      logger.info(s"going to export data to $path")

      if (path.startsWith("file:")) {
        val p = Paths.get(URI.create(path))
        if (Files.exists(p)) {
          Files.walk(p)
            .iterator()
            .asScala
            .toList
            .sorted(Ordering[Path].reverse)
            .foreach(f => Files.delete(f))
        }
      } else {
        val hdPath = new hadoop.fs.Path(path)
        if (hdfs.exists(hdPath)) {
          hdfs.delete(hdPath, true)
        }
      }

      data.samples(conf.samples).saveAsTextFile(path)

    }
    if (conf.save || conf.cache) {
      data.saveAsObjectFile(conf.path)
    }
  }
}

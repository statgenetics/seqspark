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

import java.io.File
import java.nio.file.{Files, Paths}

object ConfigValue {
  object GenomeBuild extends Enumeration {
    val hg18 = Value("hg18")
    val hg19 = Value("hg19")
    val hg38 = Value("hg38")
  }

  object GenotypeFormat extends Enumeration {
    val vcf = Value("vcf")
    val imputed = Value("impute2")
    //val bgen = Value("bgen")
    //val cacheFullvcf = Value("cachedFullVcf")
    val cacheVcf = Value("cachedVcf")
    val cacheImputed = Value("cachedImpute2")
  }

  sealed trait Samples
  object Samples {
    case object all extends Samples
    case object none extends Samples
    case class by(batch: String) extends Samples

    def apply(value: String): Samples = value match {
      case "all" => all
      case "none" => none
      case x => by(x)
    }
  }

  sealed trait Variants
  object Variants {
    case object all extends Variants
    case object exome extends Variants
    case class by(regions: String) extends Variants
    case class from(file: File) extends Variants

    def apply(value: String): Variants = value match {
      case "all" => all
      case "exome" => exome
      case x =>
        val path = Paths.get(x)
        if (Files.exists(path)) from(path.toFile) else by(x)
    }

  }

  object MutType extends Enumeration {
    val snv = Value("snv")
    val indel = Value("indel")
    val cnv = Value("cnv")
  }

  object MethodType extends Enumeration {
    val skat = Value("skat")
    val skato = Value("skato")
    val meta = Value("meta")
    val cmc = Value("cmc")
    val brv = Value("brv")
    val snv = Value("snv")
  }

  object WeightMethod extends Enumeration {
    val none = Value("none")
    val equal = Value("equal")
    val wss = Value("wss")
    val erec = Value("erec")
    val skat = Value("skat")
    val annotation = Value("annotation")
  }

  object TestMethod extends Enumeration {
    val score = Value("score")
    //val lhr = Value("lhr")
    val wald = Value("wald")
  }

  object ImputeMethod extends Enumeration {
    val bestGuess = Value("bestGuess")
    val meanDosage = Value("meanDosage")
    val random = Value("random")
    val no = Value("no")
  }

  object DBFormat extends Enumeration {
    val vcf: Value = Value("vcf")
    val gene: Value = Value("gene")
    val plain: Value = Value("plain")
    val csv: Value = Value("csv")
    val tsv: Value = Value("tsv")
  }
}

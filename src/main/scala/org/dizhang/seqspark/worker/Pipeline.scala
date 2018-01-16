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

package org.dizhang.seqspark.worker

import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.ds.Genotype.Imp
import org.dizhang.seqspark.util.UserConfig.GenotypeFormat
import org.dizhang.seqspark.util.SeqContext
//import scalaz.{Free, ~>, Id, Coyoneda}
//import scalaz.syntax.traverse._


object Pipeline {

  /**
  sealed trait Worker[A]
  final case class VCFImporter(ssc: SeqContext) extends Worker[Data[String]]
  final case class VCFCacheLoader(ssc: SeqContext) extends Worker[Data[Byte]]
  final case class ImputeImporter(ssc: SeqContext) extends Worker[Data[Imp]]
  final case class Annotater1(input: Data[String]) extends Worker[Data[String]]
  final case class Annotater2(input: Data[Byte]) extends Worker[Data[Byte]]
  final case class Annotater3(input: Data[Imp]) extends Worker[Data[Imp]]
  final case class QC1(input: Data[String]) extends Worker[Data[Byte]]
  final case class QC2(input: Data[Byte])

  //sealed trait
  */

  val a: String = "a"
  val b: Byte = 'b'
  val c: Imp = (0, 0, 0)

  def apply(implicit ssc: SeqContext): Unit = {
    val conf = ssc.userConfig
    conf.input.genotype.format match {
      case GenotypeFormat.vcf =>
        val data = Import(ssc, a)
        val annotated = Annotation(data, a)
        val clean = QualityControl(annotated, a, b)
        Export(clean)
        Association(clean)
        //run[String, Byte](data, a, b)
      case GenotypeFormat.imputed =>
        val data = Import(ssc, c)
        val annotated = Annotation(data, c)
        val clean = QualityControl(annotated, c, c)
        Export(clean)
        Association(clean)
      case GenotypeFormat.`cacheVcf` =>
        val data = Import(ssc, b)
        val annotated = Annotation(data, b)
        val clean = QualityControl(annotated, b, b)
        Export(clean)
        Association(clean)
      case GenotypeFormat.cacheImputed =>
        val data = Import(ssc, c)
        val annotated = Annotation(data, c)
        val clean = QualityControl(annotated, c, c)
        Export(clean)
        Association(clean)
    }
  }
  def run[A, B](data: Data[A], a: A, b: B)
               (implicit ssc: SeqContext,
                genoa: Genotype[A],
                genob: Genotype[B],
                importer: Import[A],
                qc: QualityControl[A, B]): Unit = {
    val conf = ssc.userConfig
    var pipeline = ssc.userConfig.pipeline

    val imported = Import(ssc, a)

    val annotated = Annotation(imported, a)

    val clean = if (pipeline.nonEmpty && pipeline.head == "qualityControl") {
      pipeline = pipeline.tail
      QualityControl(annotated, a, b)
    } else {
      QualityControl(annotated, a, b)
    }


    if (conf.qualityControl.save) {
      clean.cache()
      clean.saveAsObjectFile(conf.input.genotype.path + s".${conf.project}")
    }
    if (conf.qualityControl.export) {
      clean.cache()
      clean.saveAsTextFile(conf.input.genotype.path + s".${conf.project}.vcf")
    }

    if (pipeline.nonEmpty && pipeline.head == "association") {
      Association(clean)
    }

  }

}
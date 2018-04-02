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

import org.dizhang.seqspark.ds.{Genotype, Phenotype}
import org.dizhang.seqspark.ds.Genotype.Imp
import org.dizhang.seqspark.util.{ConfigValue => CV}
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
      case CV.GenotypeFormat.vcf =>
        run[String, Byte](a, b)
      case CV.GenotypeFormat.imputed =>
        run[Imp, Imp](c, c)
      case CV.GenotypeFormat.cacheVcf =>
        run[Byte, Byte](b, b)
      case CV.GenotypeFormat.cacheImputed =>
        run[Imp, Imp](c, c)
    }
  }



  def run[A, B](a: A, b: B)
               (implicit ssc: SeqContext,
                genoa: Genotype[A],
                genob: Genotype[B],
                importer: Import[A],
                qc: QualityControl[A, B]): Unit = {

    val conf = ssc.userConfig

    var pipeline = ssc.userConfig.pipeline

    val imported = Import(ssc, a)

    /** if select samples */
    conf.input.phenotype.samples match {
      case CV.Samples.by(b) => Phenotype.select(b, "phenotype")(ssc.sparkSession)
      case _ => Unit
    }

    if (pipeline.isEmpty) {
      Export(imported)
    } else {
      val annotated =
        if (pipeline.nonEmpty && (pipeline.head == "annotation" || pipeline.contains("association"))) {
          pipeline = if (pipeline.head == "annotation")
            pipeline.tail
          else
            pipeline
          Annotation(imported, a)
        } else {
          imported
        }

      if (pipeline.isEmpty) {
        Export(annotated)
      } else {
        val clean =
          if (pipeline.nonEmpty && pipeline.head == "qualityControl") {
            pipeline = pipeline.tail
            QualityControl(annotated, a, b)
          } else {
            QualityControl(annotated, a, b, pass = true)
          }

        if (pipeline.isEmpty) {
          Export(clean)
        } else {
          if (pipeline.nonEmpty && pipeline.head == "association") {
            pipeline = pipeline.tail
            Association(clean)
          }
          Export(clean)
        }
      }
    }

  }

}
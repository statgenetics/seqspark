/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.seqspark.parser

import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.parser.Interpreter.Eval
import org.dizhang.seqspark.variant.Variant

import scala.language.{existentials, higherKinds}

trait VariantAlg[num, repr[_]] extends ExprAlg[num, repr] {
  def chr: repr[String]
  def pos: repr[Int]
  def ref: repr[String]
  def alt: repr[String]
  def id: repr[String]
  def qual: repr[String]
  def filter: repr[String]
  def info: repr[Map[String, String]]

  def maf(): repr[Double]
  def maf(batch: repr[String]): repr[List[Double]]
  def informative(): repr[Boolean]
  def informative(batch: repr[String]): repr[List[Boolean]]
  def callRate(): repr[Double]
  def callRate(batch: repr[String]): repr[List[Double]]

  def hwePvalue(): repr[Double]
  def hwePvalue(batch: repr[String]): repr[Double]

}

object VariantAlg {

  class VariantEval[A: Genotype](v: Variant[A]) extends VariantAlg[Eval] {

  }

}

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
import cats.arrow.FunctionK
import cats.{Applicative, Monad, Monoid, FlatMap, Semigroup}
import cats.data.Const
import scala.language.higherKinds

trait ExprOptimizer[Alg[_[_]], repr[_]] {

  type M

  def monoidM: Monoid[M]
  def monadRepr: Monad[repr]

  def extract: Alg[Const[M, ?]]
  def rebuild(m: M, alg: Alg[repr]): repr[Alg[repr]]

  def optimize[A](p: ExprProgram[Alg, ])

}

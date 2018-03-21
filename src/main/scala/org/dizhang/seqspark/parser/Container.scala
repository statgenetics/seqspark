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
import cats.data.Const
import cats.{Applicative, FlatMap, Monad, Monoid, Semigroup}
trait Container[T]

object Container {

  case class Eval[T](value: T) extends Container[T]

  def eval[T]: Eval[T] => T = _.value

  case class View[T](info: String) extends Container[T]

  def view[T]: View[T] => String = _.info


  case class BatchCall(name: String, batch: String)

  /** variables, function calls without argument, and function calls with one argument */
  type Cache = (Set[String], Set[String], Set[BatchCall])

  val emptyCache = (Set[String](), Set[String](), Set[BatchCall]())

  implicit object cacheMonoid extends Monoid[Cache] {
    def empty: Cache = emptyCache
    def combine(x: Cache, y: Cache): Cache = {
      (x._1 ++ y._1, x._2 ++ y._2, x._3 ++ y._3)
    }
  }

  case class Names[T](cache: Cache) extends Container[T]

  def names[T]: Names[T] => Cache = _.cache

}

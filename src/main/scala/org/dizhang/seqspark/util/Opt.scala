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

package org.dizhang.seqspark.util

import scala.language.implicitConversions

/**
 * borrow from http://stackoverflow.com/questions/4199393/are-options-and-named-default-arguments-like-oil-and-water-in-a-scala-api/4199579#4199579
 * Aaron Novstrup's answer
 * So we can define real optional parameters, without using Option()/Some() on the client side
 */

class Opt[A] private (val option: Option[A])
object Opt {
  implicit def any2opt[A](a: A): Opt[A] = new Opt(Option(a))
  implicit def option2opt[A](o: Option[A]): Opt[A] = new Opt(o)
  implicit def opt2option[A](o: Opt[A]): Option[A] = o.option
}

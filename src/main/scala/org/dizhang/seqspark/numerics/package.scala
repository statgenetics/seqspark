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

package org.dizhang.seqspark

/**
  * Created by zhangdi on 12/13/16.
  */
package object numerics {
  val s: Stream[Double] = 1.0 #:: s.map(f => f / 2.0)
  val EPSILON: Double = s.takeWhile(e => e + 1.0 != 1.0).last
  val MINVALUE: Double = Double.MinPositiveValue
  val MAXVALUE: Double = Double.MaxValue
}

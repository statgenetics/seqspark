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

package org.dizhang.seqspark.numerics

import org.scalatest.{FlatSpec, Matchers}
import Qpsrt._
/**
  * Created by zhangdi on 12/13/16.
  */
class QpsrtSpec extends FlatSpec with Matchers {
  val error = Integrate.Memory(100)
  "A Qpsrt" should "behave well" in {
    error.init(0,1)
    error.pushFirst(0.2, 0.1)


    for (i<- 0 until error.size) {
    //  println(s"e: ${error.eList(i)} o: ${error.order(i)}")
    }
    //println(s"max: ${error.maxErr} nr: ${error.nrMax}")
  }
}

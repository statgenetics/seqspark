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
import Integrate.Memory
/**
  * Created by zhangdi on 12/13/16.
  */
object Qpsrt {

  def apply(error: Memory): Unit = {

    val last = error.size - 1
    val limit = error.limit
    val eList = error.eList
    val order = error.order

    var errMax: Double = 0.0
    var errMin: Double = 0.0
    var (i,k,top) = (0,0,0)

    var iNrMax = error.nrMax
    var iMaxErr = order(iNrMax)

    if (last < 2) {
      order(0) = 0
      order(1) = 1
    } else {
      errMax = eList(error.maxErr)

      /** this is only executed when the integrand is bad */

      while (iNrMax > 0 && errMax > eList(order(iNrMax - 1))) {
        order(iNrMax) = order(iNrMax - 1)
        iNrMax -= 1
      }
      /**
        * compute the number of elements in the list to be maintained
        * in descending order
        * */
      if (last < (limit/2 + 2)) {
        top = last
      } else {
        top = limit - last + 1
      }

      /** insert errMax by traversing the list top-down */
      i = iNrMax + 1
      while (i < top && errMax < eList(order(i))) {
        order(i - 1) = order(i)
        i += 1
      }
      order(i - 1) = iMaxErr
      /** inser errMin by traversing the list bottom-up */
      errMin = eList(last)
      k = top - 1
      while (k > i - 2 && errMin >= eList(order(k))) {
        order(k + 1) = order(k)
        k -= 1
      }
      order(k + 1) = last
      iMaxErr = order(iNrMax)
      error.maxErr = iMaxErr
      error.nrMax = iNrMax
    }
  }
}

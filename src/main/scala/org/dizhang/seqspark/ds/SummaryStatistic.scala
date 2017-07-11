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

package org.dizhang.seqspark.ds

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.assoc.SumStat
import scala.language.implicitConversions

/**
  * for rare metal
  */
class SummaryStatistic(val statistic: RDD[SumStat.RMWResult]) {

  def add(that: SummaryStatistic): SummaryStatistic = {
    val p1 = this.statistic.map(r => (r.segmentId, r))
    val p2 = that.statistic.map(r => (r.segmentId, r))
    val sum = p1.fullOuterJoin(p2).map{
      case (k, (Some(a), Some(b))) => a ++ b
      case (k, (Some(a), None)) => a
      case (k, (None, Some(b))) => b
    }

    new SummaryStatistic(sum)

  }

}
object SummaryStatistic {

  implicit def toSummaryStatistic(res: RDD[SumStat.RMWResult]): SummaryStatistic = {
    new SummaryStatistic(res)
  }

  
}

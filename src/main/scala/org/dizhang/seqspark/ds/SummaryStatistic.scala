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
import org.dizhang.seqspark.assoc.RareMetalWorker
import org.dizhang.seqspark.ds.SummaryStatistic._

/**
  * for rare metal
  */
trait SummaryStatistic {

  def `trait`: String
  def statistic: RDD[RareMetalWorker.DefaultRMWResult]
  def ++(that: SummaryStatistic): SummaryStatistic = {
    val p1 = this.statistic.map(r => (r.segmentId, r))
    val p2 = that.statistic.map(r => (r.segmentId, r))
    val sum = p1.fullOuterJoin(p2).map{
      case (k, (Some(a), Some(b))) => a ++ b
      case (k, (Some(a), None)) => a
      case (k, (None, Some(b))) => b
    }

    DefaultStatistics(`trait`, sum.asInstanceOf[RDD[RareMetalWorker.DefaultRMWResult]])

  }

}
object SummaryStatistic {

  def merge(s1: RDD[(Int, RareMetalWorker.RMWResult)],
            s2: RDD[(Int, RareMetalWorker.RMWResult)]): RDD[(Int, RareMetalWorker.RMWResult)] = {
    s1.join(s2).map{
      case (k, (r1, r2)) => (k, r1 ++ r2)
    }
  }

  case class DefaultStatistics(`trait`: String,
                               statistic: RDD[RareMetalWorker.DefaultRMWResult]) extends SummaryStatistic

  
}

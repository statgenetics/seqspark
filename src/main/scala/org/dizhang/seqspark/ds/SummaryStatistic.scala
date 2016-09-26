package org.dizhang.seqspark.ds

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.assoc.RareMetalWorker
import SummaryStatistic._

/**
  * for rare metal
  */
trait SummaryStatistic {

  def `trait`: String
  def statistic: RDD[RareMetalWorker.DefaultResult]
  def ++(that: SummaryStatistic): SummaryStatistic = {
    val p1 = this.statistic.map(r => (r.segmentId, r))
    val p2 = that.statistic.map(r => (r.segmentId, r))
    val sum = p1.fullOuterJoin(p2).map{
      case (k, (Some(a), Some(b))) => a ++ b
      case (k, (Some(a), None)) => a
      case (k, (None, Some(b))) => b
    }

    DefaultStatistics(`trait`, sum.asInstanceOf[RDD[RareMetalWorker.DefaultResult]])

  }

}
object SummaryStatistic {

  def merge(s1: RDD[(Int, RareMetalWorker.Result)],
            s2: RDD[(Int, RareMetalWorker.Result)]): RDD[(Int, RareMetalWorker.Result)] = {
    s1.join(s2).map{
      case (k, (r1, r2)) => (k, r1 ++ r2)
    }
  }

  case class DefaultStatistics(`trait`: String,
                               statistic: RDD[RareMetalWorker.DefaultResult]) extends SummaryStatistic

  
}

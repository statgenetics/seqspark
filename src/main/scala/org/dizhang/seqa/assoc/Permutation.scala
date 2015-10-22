package org.dizhang.seqa.assoc

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dizhang.seqa.ds.Counter.CounterElementSemiGroup._
import org.dizhang.seqa.stat.LinearRegression
import org.dizhang.seqa.util.Opt
import scala.annotation.tailrec

/**
 * Permutation framework
 */

object Permutation {
  type PValue = Array[PairInt]

  def merge(p1: PValue, p2: PValue): PValue =
    p1.zip(p2).map(x => PairInt.op(x._1, x._2))

  def addInPlace(pv: PValue, pi:(Int, PairInt)): PValue = {
    pv(pi._1) = PairInt.op(pv(pi._1), pi._2)
    pv
  }


  @tailrec def expand(src: RDD[(Int, DenseVector[Double])], factor: Int, n: Int): RDD[(Int, DenseVector[Double])] = {
    if (n == 0)
      src
    else {
      val target = src.map(x => Array.fill(factor)(x)).flatMap(x => x)
      expand(target, factor, n - 1)
    }
  }
}

class Permutation(y: DenseVector[Double],
                  xs: RDD[(Int, DenseVector[Double])],
                  cov: Opt[DenseMatrix[Double]] = None)
                 (implicit sc: SparkContext)
  extends Serializable {

  /** parameters for adaptive permutation */
  val m = min( xs.count().toInt, 1000000 )
  val alpha = 0.05 / m
  val b = 2000 * m - 100
  val r = 121
  val factor  = 10

  def summary = {
    val analyticModel = xs.map(x => (x._1, new LinearRegression(y, x._2, cov)))
    val analyticSummary = sc.broadcast(analyticModel.map(x => (x._1, x._2.summary)).collect().sortBy(_._1).map(_._2))
    val analyticStatistic = sc.broadcast(analyticModel.map(x => (x._1, x._2.summary.statistics(1))).collect().sortBy(_._1).map(_._2))
    val accumZero = Array.fill(m)(PairInt.zero)
    var accum = accumZero
    val factor = 10
    val sequential =  r * factor
    val loopCnt: Int = math.round(math.log(m)/math.log(factor)).toInt
    var tested = 0
    var cur = xs
    for (i <- 0 to loopCnt) {
      var testCnt = math.pow(factor, i).toInt
      if (testCnt >= b)
        testCnt = b - sum(0 to i map (x => math.pow(factor, x).toInt * r * factor))
      val curStatistic = Permutation.expand(cur, factor, i)
        .map(x => {
          val model = new LinearRegression(shuffle(y), x._2, cov)
          val summary = model.summary
          val statistic: Double = summary.statistics(1)
          val cnt = if (statistic > analyticStatistic.value(x._1))
            (1, 1)
          else
            (0, 1)
          (x._1, cnt)})
        .aggregate(Array.fill(m)(PairInt.zero))(Permutation.addInPlace, Permutation.merge)
      accum = Permutation.merge(accum, curStatistic)
      cur = cur.filter(x => accum(x._1)._1 < r)
    }
    val pvalues = accum.map(x => x._1.toDouble / x._2)
    pvalues
  }

}

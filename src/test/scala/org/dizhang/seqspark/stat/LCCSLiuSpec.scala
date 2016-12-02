package org.dizhang.seqspark.stat

import breeze.linalg.DenseVector
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhangdi on 11/24/16.
  */
class LCCSLiuSpec extends FlatSpec with Matchers {

  trait Data {
    val dis2 = LCCSLiu.Modified(DenseVector(61647.171903,7.266117,6.872818,     6.608951,     6.569637 ,    6.516321,
      6.406619 ,    6.284201  ,   6.027278    , 5.939418))
    val dis = LCCSLiu.Modified(DenseVector(3771.8800871369217,3827.6802635442027,3990.8395006717205,
      4068.5838736790965,4138.261764764718,4172.109564262479,4197.06479805482,
      4364.667266210289,4614.401733626494,1924088.423425927))
  }

  "A LiuModified" should "work fine" in new Data {

    //dis.cdf(32189.63).pvalue should be ((1.0 - 0.4703215) +- 1e-4)
    val cutoff2 = 32189.63
    val cutoff = 1025077.779555519
    val norm =  (cutoff - dis.muQ)/dis.sigmaQ
    val norm1 = norm * dis.sigmaX + dis.muX
    //println(s"df: ${dis.df} delta: ${dis.delta} norm1: ${norm1} pvalue: ${dis.cdf(cutoff).pvalue}")
    //println(s"muQ:${dis.muQ} sigmaQ:${dis.sigmaQ} muX: ${dis.muX} sigmaX: ${dis.sigmaX}")
  }
}

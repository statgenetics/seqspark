package org.dizhang.seqspark

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.Variant
import org.dizhang.seqspark.util.SingleStudyContext

import scalaz._
/**
  * Created by zhangdi on 9/19/16.
  */
package object worker {
  type Data[A] = RDD[Variant[A]]

}

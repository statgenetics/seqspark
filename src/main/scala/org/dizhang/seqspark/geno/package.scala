package org.dizhang.seqspark

import org.apache.spark.rdd.RDD
import org.dizhang.seqspark.ds.{Phenotype, Variant}
import org.dizhang.seqspark.util._
import scalaz._
/**
  * Created by zhangdi on 8/13/16.
  */
package object geno {
  type Data[A] = RDD[Variant[A]]



}

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

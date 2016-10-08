package org.dizhang.seqspark.util

import org.scalatest.FlatSpec

/**
  * Created by zhangdi on 10/7/16.
  */
class LogicalExpressionSpec extends FlatSpec {

  "Single term" should "work fine" in {
    val x = "isFunctional"
    LogicalExpression.analyze(x)
  }

}

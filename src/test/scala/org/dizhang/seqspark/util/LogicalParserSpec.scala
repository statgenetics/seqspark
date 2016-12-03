package org.dizhang.seqspark.util

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhangdi on 12/3/16.
  */
class LogicalParserSpec extends FlatSpec with Matchers {
  "A LogicalParser" should "be able to constructed" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38")
  }

  "A LogicalParser" should "eval to true" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38")
    LogicalParser.eval(lp)(Map("INFO.AN"->"3900", "INFO.AC"->"40")) should be (true)
  }
  "A LogicalParser" should "eval to false" in {
    val lp = LogicalParser.parse("INFO.AN>3800 and INFO.AC>38 and INFO.AC<3750")
    LogicalParser.eval(lp)(Map("INFO.AN"->"3900", "INFO.AC"->"3775")) should be (false)
  }

}

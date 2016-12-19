package org.dizhang.seqspark.meta

import org.dizhang.seqspark.assoc.RareMetalWorker

/**
  * meta analysis
  */
trait Burden {

  def worker: RareMetalWorker.RMWResult


}

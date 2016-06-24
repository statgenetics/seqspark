package org.dizhang.seqspark.meta

import org.dizhang.seqspark.assoc.RareMetalWorker
import org.dizhang.seqspark.ds.Variation

/**
  * meta analysis
  */
trait Burden {

  def worker: RareMetalWorker.Result


}

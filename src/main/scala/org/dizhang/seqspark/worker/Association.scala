package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.dizhang.seqspark.assoc.AssocMaster
import org.dizhang.seqspark.util.InputOutput._
import java.util.logging
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.worker.WorkerObsolete.Data

/**
 * Association testing
 */

object Association extends WorkerObsolete[Data, Data] {

  implicit val name = new WorkerName("association")

  def apply(data: Data)(implicit cnf: RootConfig, sc: SparkContext): Data = {

    val (input, pheno) = data

    /** mute the netlib-java info level logging */
    val flogger = logging.Logger.getLogger("com.github.fommil")
    flogger.setLevel(logging.Level.WARNING)
    val assoc = new AssocMaster(input, pheno)
    assoc.run()
    (input, pheno)
  }
}

package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqa.assoc.{AssocTest, Phenotype}
import org.dizhang.seqa.stat.LogisticRegression
import org.dizhang.seqa.util.Constant
import org.dizhang.seqa.util.InputOutput._
import java.util.logging
/**
 * Association testing
 */

object Association extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("association")

  /** Constants */
  val CPPheno = Constant.ConfigPath.SampleInfo.source

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {

    /** mute the netlib-java info level logging */
    val flogger = logging.Logger.getLogger("com.github.fommil")
    flogger.setLevel(logging.Level.WARNING)
    val phenoFile = cnf.getString(CPPheno)
    val phenotype = Phenotype(phenoFile)
    val assoc = new AssocTest(input, phenotype)
    assoc.run
    input

  }
}

package org.dizhang.seqspark.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.assoc.{AssocTest, Phenotype}
import org.dizhang.seqspark.ds.Genotype
import org.dizhang.seqspark.stat.LogisticRegression
import org.dizhang.seqspark.util.Constant
import org.dizhang.seqspark.util.InputOutput._
import java.util.logging
/**
 * Association testing
 */

object Association extends Worker[Genotype, Genotype] {

  implicit val name = new WorkerName("association")

  /** Constants */
  val CPPheno = Constant.ConfigPath.SampleInfo.source

  def apply(geno: Genotype)(implicit cnf: Config, sc: SparkContext): Genotype = {

    val input = geno.data

    /** mute the netlib-java info level logging */
    val flogger = logging.Logger.getLogger("com.github.fommil")
    flogger.setLevel(logging.Level.WARNING)
    val phenoFile = cnf.getString(CPPheno)
    val phenotype = Phenotype(phenoFile)
    val assoc = new AssocTest(input, phenotype)
    assoc.run()
    geno
  }
}

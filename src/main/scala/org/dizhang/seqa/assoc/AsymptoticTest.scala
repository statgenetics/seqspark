package org.dizhang.seqa.assoc

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.dizhang.seqa.util.InputOutput.VCF

/**
  * asymptotoc test
  */
class AsymptoticTest(genotype: VCF, phenotype: Phenotype)(implicit config: Config, sc: SparkContext) extends Assoc {
  def runTrait(traitName: String) = {
    try {
      logger.info(s"load trait $traitName from phenotype database")
      val y = phenotype.makeTrait(traitName)
      
    } catch {
      case e: Exception => logger.warn(e.getStackTrace.toString)
    }
  }
}

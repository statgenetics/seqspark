package org.dizhang.seqa.assoc

import org.dizhang.seqa.assoc.Phenotype

import scala.collection.JavaConverters._
import org.dizhang.seqa.util.Configuration
import org.dizhang.seqa.util.InputOutput.VCF

/**
  * General burden method
  */
object Burden {

}

class Burden(genotype: VCF, phenotype: Phenotype)(implicit val conf: Configuration) extends Assoc {
  def runTrait(t: String, funcVars: VCF): Unit = {
    logger.info("load trait from phenotype")
    val y = phenotype.makeTrait(t)
    val cov = phenotype.makeCov(t, cnf.getStringList(s"association.trait.$t.covariates").asScala.toList.mkString("\t"))
    val x =
  }
}

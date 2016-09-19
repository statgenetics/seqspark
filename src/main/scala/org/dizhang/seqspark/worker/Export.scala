package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.dizhang.seqspark.annot.{RefGene, Regions}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.worker.WorkerObsolete.Data

/**
 * Export genotype data
 */

object Export extends WorkerObsolete[Data, Unit] {

  implicit val name = new WorkerName("export")
  type Buffer = Array[Byte]

  def writePheno(pheno: Phenotype, samples: Either[Samples.Value, String], path: String): Unit = {
    samples match {
      case Left(Samples.all) => Phenotype.save(pheno, path)
      case Right(field) => Phenotype.save(pheno.filter(field), path)
      case _ => {}
    }
  }

  def writeGeno(geno: VCF, samples: Either[Samples.Value, Array[Boolean]], path: String): Unit = {
    samples match {
      case Left(Samples.all) => VCF.save(geno, path)
      case Left(Samples.none) => VCF.save(geno.toDummy, path)
      case Left(_) => VCF.save(geno.toDummy, path)
      case Right(b) => VCF.save(geno.select(b), path)
    }
  }

  def apply(data: Data)(implicit cnf: RootConfig, sc: SparkContext): Unit = {

    val (geno, pheno) = data
    val exCnf = cnf.export
    geno.updateConfig(exCnf)
    val samples = exCnf.samples match {
      case Left(a) => Left(a)
      case Right(b) => Right(pheno.indicate(b))
    }

    writePheno(pheno, exCnf.samples, exCnf.sampleInfo)
    exCnf.variants match {
      case Left(Variants.exome) => {
        val coordFile = cnf.annotation.geneCoord
        val exome = sc.broadcast(Regions.makeExome(coordFile))
        writeGeno(geno.filter(exome), samples, exCnf.path)
      }
      case Left(Variants.all) => writeGeno(geno, samples, exCnf.path)
      case Left(_) => {}
      case Right(b) => writeGeno(geno.filter(b), samples, exCnf.path)
    }
  }
}



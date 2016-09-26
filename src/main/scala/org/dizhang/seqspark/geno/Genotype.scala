package org.dizhang.seqspark.geno

import RawVCF._
import SimpleVCF._
import ImputedVCF._

/**
  * Created by zhangdi on 9/26/16.
  */
/** here we go */

trait Genotype[A] {
  def maf(g: A): (Double, Double)
  def toCMC(g: A, maf: Double): Double
  def toBRV(g: A, maf: Double): Double
  def callRate(g: A): (Double, Double)
}

object Genotype {
  type Imp = (Double, Double, Double)
  implicit object Simple extends Genotype[Byte] {
    def maf(g: Byte) = g.maf
    def toCMC(g: Byte, maf: Double) = g.toCMC(maf)
    def toBRV(g: Byte, maf: Double) = g.toBRV(maf)
    def callRate(g: Byte) = g.callRate
  }
  implicit object Raw extends Genotype[String] {
    def maf(g: String) = g.maf
    def toCMC(g: String, maf: Double) = g.toSimpleGenotype.toCMC(maf)
    def toBRV(g: String, maf: Double) = g.toSimpleGenotype.toBRV(maf)
    def callRate(g: String) = g.callRate
  }
  implicit object Imputed extends Genotype[Imp] {
    def maf(g: Imp) = g.maf
    def toCMC(g: Imp, maf: Double) = g.toCMC(maf)
    def toBRV(g: Imp, maf: Double) = g.toBRV(maf)
    def callRate(g: Imp) = g.callRate
  }

}

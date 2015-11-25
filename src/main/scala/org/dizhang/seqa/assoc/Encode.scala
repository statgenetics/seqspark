package org.dizhang.seqa.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.pow
import com.typesafe.config.Config
import org.dizhang.seqa.ds.Counter
import org.dizhang.seqa.stat.{LinearRegression, LogisticRegression}
import org.dizhang.seqa.util.Constant.Annotation
import org.dizhang.seqa.util.Constant.UnPhased.Bt
import org.dizhang.seqa.util.InputOutput.Var
import org.dizhang.seqa.worker.GenotypeLevelQC.{getMaf, makeMaf}
import Encode._

/**
  * How to code variants in one group (gene)
  */
object Encode {
  def cmcMakeNaAdjust(x: Byte, maf: Double): Double =
    x match {
      case Bt.ref => 0.0
      case Bt.het1 => 1.0
      case Bt.het2 => 1.0
      case Bt.mut => 1.0
      case _ => 1.0 - math.pow(1.0 - maf, 2)}

  object CmcAddNaAdjust extends Counter.CounterElementSemiGroup[Double] {
    def zero = 0.0

    def pow(x: Double, i: Int) =
      x match {
        case 1.0 => 1.0
        case 0.0 => 0.0
        case _ => 1.0 - math.pow(1.0 - x, i)}

    def op(a: Double, b: Double) =
      if (a == 1.0 || b == 1.0)
        1.0
      else
        1.0 - (1.0 - a) * (1.0 - b)}

  def brvMakeNaAdjust(x: Byte, maf: Double): Double =
    x match {
      case Bt.ref => 0.0
      case Bt.het1 => 1.0
      case Bt.het2 => 1.0
      case Bt.mut => 2.0
      case _ => 2 * maf
    }

  val BrvAddNaAdjust = Counter.CounterElementSemiGroup.AtomDouble

  /** The authors just said the sample size needed is very large, but not how large*/
  def erecDelta(n: Int): Double = {
    if (n < 2000)
      1.0
    else if (n > 100000)
      0.1
    else
      0.1 + 0.9 * (n - 2000) / 98000
  }

}

sealed trait Encode {
  def vars: Iterable[Var]
  def controls: Array[Boolean]
  def config: Config
  def maf: Array[Double] = {
    config.getString("maf.source") match {
      case "pooled" =>
        vars.map(v => getMaf(v.toCounter(makeMaf, (0, 2)).reduce)).toArray
      case "controls" =>
        vars.map(v => getMaf(v.select(controls).toCounter(makeMaf, (0, 2)).reduce)).toArray
      case "annotation" =>
        vars.map(v => v.parseInfo(Annotation.mafInfo).toDouble).toArray}}
  implicit lazy val fixedCutoff: Double = config.getDouble("maf.cutoff")
  def thresholds: Array[Double] = {
    val n = controls.length
    /** this is to make sure 90% call rate sites is considered the same with the 100% cr ones
      * (with 1 - 4 minor alleles. anyway 5 is indistinguishable.
      * Because 5.0/0.9n - 5.0/n == 5.0/n - 4.0/0.9n */
    val tol = 4.0/(9 * n)
    val sortedMaf = maf.sorted
    sortedMaf.map(c => Array(c)).reduce((a, b) =>
      if (a(-1) + tol >= b(0))
        a.slice(0, a.length - 1) ++ b
      else
        a ++ b)
  }
  def getFixed(implicit cutoff: Double): DenseVector[Double]
  def getVT = {
    DenseVector.horzcat(thresholds.map(c => this.getFixed(c)): _*)
  }
}

case class CMC(vars: Iterable[Var],
               y: DenseVector[Double],
               cov: Option[DenseMatrix[Double]],
               controls: Array[Boolean],
               config: Config) extends Encode {

  def getFixed(implicit cutoff: Double): DenseVector[Double] = {
    vars.zip(maf).filter(v => v._2 < cutoff).map(v =>
      v._1.toCounter(cmcMakeNaAdjust(_, v._2), 0.0)
    ).reduce((a, b) => a.++(b)(CmcAddNaAdjust)).toDenseVector(x => x)}



}

case class BRV(vars: Iterable[Var],
               y: DenseVector[Double],
               cov: Option[DenseMatrix[Double]],
               controls: Array[Boolean],
               config: Config) extends Encode {
  private def getGenotype(implicit cutoff: Double) = {
    val tmp = vars.zip(maf).filter(v => v._2 < cutoff).map(v =>
      v._1.toCounter(brvMakeNaAdjust(_, v._2), 0.0).toArray).toArray
    DenseMatrix(tmp: _*).t}

  private def weight(cutoff: Double): Option[DenseVector[Double]] = {
    /** weight length should be equal to the size of vars after maf filtering */
    config.getString("weight") match {
      case "equal" => None
      case "annotation" =>
        Some(DenseVector(vars.map(v => v.parseInfo(Annotation.weightInfo).toDouble).zip(maf)
          .filter(v => v._2 < cutoff).map(_._1).toArray))
      case "maf" =>
        val mafDV = DenseVector(this.maf.filter(m => m < cutoff))
        Some(pow(mafDV :* mafDV.map(1.0 - _), -0.5))
      case "erec" => {
        val n = vars.size
        val combined = cov match {
          case None => getGenotype(cutoff)
          case Some(c) => DenseMatrix.horzcat(getGenotype(cutoff), c)
        }
        val beta =
          if (y.toArray.count(_ == 0.0) + y.toArray.count(_ == 1.0) == y.length) {
            val model = new LogisticRegression(y, combined)
            model.estimates.map(x => x + erecDelta(y.length))
          } else {
            val model = new LinearRegression(y, combined)
            model.estimates.map(x => x + 2 * erecDelta(y.length))
          }
        Some(beta(1 to n))
      }
    }
  }

  def getFixed(implicit cutoff: Double): DenseVector[Double] = {
    val genotype = this.getGenotype(cutoff)
    weight(cutoff) match {
      case None => genotype * DenseVector.fill(genotype.size)(1.0)
      case Some(w) => genotype * w}}
}
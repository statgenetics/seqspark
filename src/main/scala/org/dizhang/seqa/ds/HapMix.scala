package org.dizhang.seqa.ds

/**
 * HapMix format
 */
@SerialVersionUID(10L)
class HapMix(val snp: String, val geno: String) extends Serializable

object HapMix {
  def apply(): HapMix = new HapMix("", "")
  def apply[A](v: Variant[A])(implicit make: A => String): HapMix = {
    val snp = s"${v.chr}-${v.pos} ${v.chr} ${v.pos.toDouble / 1e6} ${v.pos}\n"
    val geno = v.geno(make).mkString("") + "\n"
    new HapMix(snp, geno)
  }
  def add(a: HapMix, b: HapMix): HapMix =
    new HapMix(a.snp + b.snp, a.geno + b.geno)
}
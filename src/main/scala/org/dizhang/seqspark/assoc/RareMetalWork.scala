package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseVector => DV, Vector => Vec, SparseVector => SV,}

/**
  * raremetal worker
  * generate the summary statistics
  */
trait RareMetalWork {
  def y: DV[Double]
  def yEstimate: DV[Double]
  def x: Encode
}

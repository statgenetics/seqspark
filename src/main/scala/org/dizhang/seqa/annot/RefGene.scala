package org.dizhang.seqa.annot

import org.dizhang.seqa.ds.Region

import scala.collection.immutable.TreeMap

/**
  * refgene
  */

object RefGene {
  def apply(): RefGene = {

  }
}

trait RefGene {
  val build: String
  val transcript: TreeMap[Region, String]
  val cds: TreeMap[Region, String]
  val exon: TreeMap[Region, String]
}

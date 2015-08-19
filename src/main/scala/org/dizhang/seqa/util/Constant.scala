package org.dizhang.seqa.util

import org.dizhang.seqa.ds.Region

/**
 * Created by zhangdi on 8/18/15.
 */

object Constant {

  object Pheno {
    val delim = "\t"
  }

  object Bt {
    val mis: Byte = -9
    val ref: Byte = 0
    val het: Byte = 1
    val mut: Byte = 2
    def conv(g: Byte): String = {
      g match {
        case `mis` => Gt.mis
        case `het` => Gt.het
        case `mut` => Gt.mut
        case `ref` => Gt.ref
        case _ => Gt.mis
      }
    }
    def flip(g: Byte): Byte = {
      g match {
        case `ref` => mut
        case `mut` => ref
        case _ => g
      }
    }
  }

  object Gt {
    val mis = "./."
    val ref = "0/0"
    val het = "0/1"
    val mut = "1/1"
    def conv(g: String): Byte = {
      g match {
        case `mis` => Bt.mis
        case `het` => Bt.het
        case `mut` => Bt.mut
        case `ref` => Bt.ref
        case _ => Bt.mis
      }
    }
  }

  object Hg19 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 60000, 2699519), Region("X", 154931043, 155260559))
    val pseudoY = List(Region("Y", 10000, 2649519), Region("Y", 59034049, 59363565))
    val pseudo = pseudoX ::: pseudoY
    /** use a definition of MHC region from BGI
      * url:
      * http://bgiamericas.com/service-solutions/genomics/human-mhc-seq/
      */
    val mhc = Region(6.toByte, 29691115, 33054975)
  }

  object Hg38 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 10000, 2781478), Region("X", 155701382, 156030894))
    val pseudoY = List(Region("Y", 10000, 2781478), Region("Y", 56887902, 57217414))
    val pseudo = pseudoX ::: pseudoY
  }
}

package org.dizhang.seqa.util

import org.dizhang.seqa.ds.Region

/**
 *
 */

object Constant {

  object Pheno {
    val delim = "\t"
    val mis = "NA"
  }

  object Annotation {
    val mafInfo = "SEQA_ANNO_MAF"
    val weightInfo = "SEQA_ANNO_WEIGHT"
  }

  implicit class Genotype(val value: String) extends AnyVal {
    import UnPhased._
    def gt = value.split(":")(0).replace('|', '/')
    def bt = Gt.conv(gt)
  }

  implicit class InnerGenotype(val value: Byte) extends AnyVal {
    import UnPhased._
    def isHet = value == Bt.het1 || value == Bt.het2
    def isHom = value == Bt.ref || value == Bt.mut
    def toPhased = Bt.conv(value)
    def toUnPhased = toPhased.replace('/', '|')
  }

  object UnPhased {
    object Bt {
      val mis: Byte = -9
      val ref: Byte = 0
      val het1: Byte = 1
      val het2: Byte = 2
      val mut: Byte = 3
      def conv(g: Byte): String = {
        g match {
          case `mis` => Gt.mis
          case `het1` => Gt.het1
          case `het2` => Gt.het2
          case `mut` => Gt.mut
          case `ref` => Gt.ref
          case _ => Gt.mis
        }
      }
      def flip(g: Byte): Byte = {
        g match {
          case `ref` => mut
          case `mut` => ref
          case `het1` => het2
          case `het2` => het1
          case _ => g
        }
      }
    }

    object Gt {
      val mis = "./."
      val ref = "0/0"
      val het1 = "0/1"
      val het2 = "1/0"
      val mut = "1/1"
      def conv(g: String): Byte = {
        g match {
          case `mis` => Bt.mis
          case `het1` => Bt.het1
          case `het2` => Bt.het2
          case `mut` => Bt.mut
          case `ref` => Bt.ref
          case _ => Bt.mis
        }
      }
    }
  }

  object Hg19 {
    /** 0-based closed intervals, as with Region */
    //val chr1 = Region("1", 0, 248956421)
    val pseudoX = List(Region("X", 60000, 2699519), Region("X", 154931043, 155260559))
    val pseudoY = List(Region("Y", 10000, 2649519), Region("Y", 59034049, 59363565))
    val pseudo = pseudoX ::: pseudoY
    /** use a definition of MHC region from Genome Reference Consortium
      * url:
      * http://www.ncbi.nlm.nih.gov/projects/genome/assembly/grc/human/
      */
    val mhc = Region(6.toByte, 28477796, 33448353)
  }

  object Hg38 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 10000, 2781478), Region("X", 155701382, 156030894))
    val pseudoY = List(Region("Y", 10000, 2781478), Region("Y", 56887902, 57217414))
    val pseudo = pseudoX ::: pseudoY
    val mhc = Region(6.toByte, 28510119, 33480576)
  }
}

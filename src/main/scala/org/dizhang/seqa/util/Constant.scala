package org.dizhang.seqa.util

import org.dizhang.seqa.ds.Region

/**
 *
 */

object Constant {
  object Permutation {
    val alpha = 0.05
    def max(sites: Int) = 2000 * sites - 100
    def min(sites: Int) = sites match {
      case x if x == 1 => 115
      case x if x <= 5 => 120
      case _ => 121
    }
    val base = 10

  }

  object ConfigPath {
    val sampleInfo = "sampleInfo"
    object SampleInfo {
      val source = "source"
    }

    val association = "association"
    object Association {
      val method = s"$association.method"
      object Method {
        val list = s"$method.list"
      }
      object SomeMethod {
        /** relative path */
        val maf = "maf"
        object Maf {
          val source = s"$maf.source"
          val cutoff = s"$maf.cutoff"
          val fixed = s"$maf.fixed"
        }
        val coding = "coding"
        val weight = "weight"
        val permutation = "permutation"
        val test = "test"
      }
      val `trait` = s"$association.trait"
      object Trait {
        val list = s"${`trait`}.list"
      }
      object SomeTrait {
        val binary = "binary"
        val covariates = "covariates"
      }
    }

    val annotation = "annotation"
    object Annotation {
      val refGene = s"$annotation.refGene"
      object RefGene {
        val build = s"$refGene.build"
        val coord = s"$refGene.coord"
        val seq = s"$refGene.seq"
      }
      val maf = s"$annotation.maf"
      object Maf {
        val source = s"$maf.source"
        val tag = s"$maf.tag"
        val an = s"$maf.an"
        val ac = s"$maf.ac"
      }
    }
  }

  object ConfigValue {
    object Annotation {
      object Build extends Enumeration {
        type Build = Value
        val hg19 = Value("hg19")
        val hg38 = Value("hg38")
      }
    }
    object Association {
      object SomeMethod {
        object Coding {
          val cmc = "collapse"
          val brv = "burden"
          val single = "single"
        }
        object Maf {
          object Source {
            val pooled = "pooled"
            val controls = "controls"
            val annotation = "annotation"
          }
        }
        object Weight {
          val none = "none"
          val equal = "equal"
          val wss = "wss"
          val erec = "erec"
          val annotation = "annotation"
        }
        object Test {
          val score = "score"
          val lrt = "lrt"
          val wald = "wald"
        }
      }
      object SomeTrait {
        object Type {
          val binary = "binary"
          val quant = "quant"
        }
      }
    }
  }

  object Pheno {
    object Header {
      val fid = "fid"
      val iid = "iid"
      val pid = "pid"
      val mid = "mid"
      val sex = "sex"
      val control = "control"
      val pca = "pca"
    }
    val delim = "\t"
    val mis = "NA"

  }

  object Variant {
    object InfoKey {
      val maf = "SEQA_ANNO_MAF"
      val weight = "SEQA_ANNO_WEIGHT"
      val gene = "SEQA_GENE"
    }
  }

  object Annotation {
    object Feature extends Enumeration {
      type Feature = Value
      val InterGenic = Value("InterGenic")
      val Upstream = Value("Upstream")
      val Downstream = Value("Downstream")
      val UTR5 = Value("5-UTR")
      val UTR3 = Value("3-UTR")
      val CDS = Value("CDS")
      val Intronic = Value("Intronic")
      val Exonic = Value("Exonic")
      val Synonymous = Value("Synonymous")
      val NonSynonymous = Value("NonSynonymous")
      val StopGain = Value("StopGain")
      val StopLoss = Value("StopLoss")
      val SpliceSite = Value("SpliceSite")
    }
    object Nucleotide extends Enumeration {
      type Nucleotide = Value
      val T = Value("T")
      val C = Value("C")
      val A = Value("A")
      val G = Value("G")
      val N = Value("N")
    }
    object AminoAcid extends Enumeration {
      type AminoAcid = Value
      val F = Value("F")
      val L = Value("L")
      val S = Value("S")
      val Y = Value("Y")
      val Stop = Value("Stop")
      val C = Value("C")
      val W = Value("W")
      val P = Value("P")
      val H = Value("H")
      val Q = Value("Q")
      val R = Value("R")
      val I = Value("I")
      val M = Value("M")
      val T = Value("T")
      val N = Value("N")
      val K = Value("K")
      val V = Value("V")
      val A = Value("A")
      val D = Value("D")
      val E = Value("E")
      val G = Value("G")
    }
    val codeTable: Array[AminoAcid.AminoAcid] = {
      import AminoAcid._
      Array(F, F, L, L, S, S, S, S, T, T, Stop, Stop, C, C, Stop, W,
            L, L, L, L, P, P, P, P, H, H, Q, Q, R, R, R, R,
            I, I, I, M, T, T, T, T, N, N, K, K, S, S, R, R,
            V, V, V, V, A, A, A, A, D, D, E, E, G, G, G, G)
    }
    def translate(codon: String): AminoAcid.AminoAcid = {
      require(codon.forall(_ != 'N'), "cannot translate codon containing N")
      val num = codon.map {
        case 'T' => 0
        case 'C' => 1
        case 'A' => 2
        case 'G' => 3
      }
      val idx = num(0) * 16 + num(1) * 4 + num(2)
      codeTable(idx)
    }
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
    val mhc = Region("6", 28477796, 33448353)
  }

  object Hg38 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 10000, 2781478), Region("X", 155701382, 156030894))
    val pseudoY = List(Region("Y", 10000, 2781478), Region("Y", 56887902, 57217414))
    val pseudo = pseudoX ::: pseudoY
    val mhc = Region("6", 28510119, 33480576)
  }
}

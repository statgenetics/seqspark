/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.util

import org.dizhang.seqspark.annot.IntervalTree
import org.dizhang.seqspark.ds.Region

/**
 *
 */

object Constant {
  object Permutation {
    val alpha = 0.05
    def max2(sites: Int) =  500 * sites - 25
    def min2(sites: Int) = if (sites == 1) 34 else 36
    def max(sites: Int) = 2000 * sites - 100
    def min(sites: Int) = sites match {
      case x if x == 1 => 115
      case x if x <= 5 => 120
      case _ => 121
    }
    val base = 2

  }

  object Pheno {

    val fid = "fid"
    val iid = "iid"
    val pid = "pid"
    val mid = "mid"
    val sex = "sex"
    val control = "control"
    val pcPrefix = "_pc"
    val batch = "batch"
    val delim = "\t"
    val mis = "NA"
    val t = "1"
  }

  object Variant {
    object InfoKey {
      val pass = "SS_PASS"
      val mac = "SS_MAC"
      val macAll = "SS_MAC_ALL"
      val macCtrl = "SS_MAC_CTRL"
      val mafAll = "SS_MAF_ALL"
      val mafCtrl = "SS_MAF_CTRL"
      val weight = "SS_WEIGHT"
      val gene = "SS_GENE"
      val func = "SS_FUNC"
      val anno = "SS_ANNO"
      val informative = "SS_informative"
      val maf = "SS_AF"
      val batchMaf = "SS_batchMAF"
      val callRate = "SS_callRate"
      val batchCallRate = "SS_batchCallRate"
      val hwePvalue = "SS_hwePvalue"
      val isFunctional = "SS_isFunctional"
      val batchSpecific = "SS_batchSpecific"
    }
    val dbExists = "dbExists"
  }

  object Annotation {
    object Feature extends Enumeration {
      type Feature = Value
      val StopGain = Value("StopGain")
      val StopLoss = Value("StopLoss")
      val SpliceSite = Value("SpliceSite")
      val FrameShiftIndel = Value("FrameShiftIndel")
      val NonSynonymous = Value("NonSynonymous")
      val NonFrameShiftIndel = Value("NonFrameShiftIndel")
      val Synonymous = Value("Synonymous")
      val CDS = Value("CDS")
      val Exonic = Value("Exonic")
      val UTR5 = Value("5UTR")
      val UTR3 = Value("3UTR")
      val Intronic = Value("Intronic")
      val Upstream = Value("Upstream")
      val Downstream = Value("Downstream")
      val InterGenic = Value("InterGenic")
      val CNV = Value("CNV")
      val Unknown = Value("Unknown")
    }

    object Base extends Enumeration {
      type Base = Value
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

  }


  object Genotype {
    object Raw {
      val diploidPhasedMis = ".|."
      val diploidPhasedRef = "0|0"
      val diploidPhasedHet1 = "0|1"
      val diploidPhasedHet2 = "1|0"
      val diploidPhasedMut = "1|1"
      val diploidUnPhasedMis = "./."
      val diploidUnPhasedRef = "0/0"
      val diploidUnPhasedHet1 = "0/1"
      val diploidUnPhasedHet2 = "1/0"
      val diploidUnPhasedMut = "1/1"
      val monoploidMis = "."
      val monoploidRef = "0"
      val monoploidMut = "1"
    }

    object Imputed {
      val mis = (0.0, 0.0, 0.0)
      val ref = (1.0, 0.0, 0.0)
    }

  }

  object Hg19 {
    /** 0-based closed intervals, as with Region */
    //val chr1 = Region("1", 0, 248956421)
    val pseudoX = List(Region("X", 60000, 2699519), Region("X", 154931043, 155260559))
    val pseudoY = List(Region("Y", 10000, 2649519), Region("Y", 59034049, 59363565))
    val pseudo = IntervalTree( (pseudoX ::: pseudoY).toIterator )
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
    val pseudo = IntervalTree( (pseudoX ::: pseudoY).toIterator)
    val mhc = Region("6", 28510119, 33480576)
  }
}

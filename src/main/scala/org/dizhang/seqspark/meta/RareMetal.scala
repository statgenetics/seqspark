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

package org.dizhang.seqspark.meta

import breeze.linalg.diag
import org.dizhang.seqspark.annot.{IntervalTree, RefGene}
import org.dizhang.seqspark.assoc.{RareMetalWorker => RMW}
import org.dizhang.seqspark.ds.Variation
import org.dizhang.seqspark.meta.RareMetal._
import org.dizhang.seqspark.util.Constant._
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.UserConfig.MetaConfig

/**
  * Created by zhangdi on 5/26/16.
  */

trait RareMetal {

  def config: MetaConfig

  def sum: RMW.RMWResult

  def annotate(refGene: RefGene): Unit = {
    sum.vars.map { v =>
      val annot = IntervalTree.lookup(refGene.loci, v).filter(l =>
        refGene.seq.contains(l.mRNAName)).map { l =>
        (l.geneName, l.mRNAName, l.annotate(v, refGene.seq(l.mRNAName)))
      }
      annot match {
        case Nil =>
          v.addInfo(IK.anno, F.InterGenic.toString)
        case _ =>
          val merge = annot.map(p => (p._1, s"${p._2}:${p._3.toString}")).groupBy(_._1)
            .map{case (k, value) => "%s::%s".format(k, value.map(x => x._2).mkString(","))}
            .mkString(",,")
          v.addInfo(IK.anno, merge)
      }
    }
  }

  def getSingle: Array[(Variation, Double)] = {
    val res = sum.score :/ diag(sum.variance).map(_.sqrt)
    sum.vars.zip(res.toArray)
  }

}

object RareMetal {
  val IK = Variant.InfoKey
  val F = Annotation.Feature
  val FM = F.values.zipWithIndex.toMap

}
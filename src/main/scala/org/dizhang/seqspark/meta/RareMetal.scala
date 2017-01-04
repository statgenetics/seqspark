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

  def annotate(refGene: RefGene): Map[String, Array[(String, Variation)]] = {
    sum.vars.flatMap{ v =>
      IntervalTree.lookup(refGene.loci, v)
        .map(l => (l.geneName, l.annotate(v, refGene.seq(l.mRNAName))))
        .groupBy(_._1).mapValues(x => x.map(_._2).reduce((a, b) => if (FM(a) < FM(b)) a else b))
        .toArray.map{x =>
        val newV = Variation(v.chr, v.start, v.end, v.ref, v.alt, None)
        newV.addInfo(IK.gene, x._1)
        newV.addInfo(IK.func, x._2.toString)
        (x._1, newV)}
    }.groupBy(x => x._1)
  }

  def getSingle: Array[(Variation, Double)] = {
    val res = sum.score :/ diag(sum.variance).map(_.sqrt)
    sum.vars.zip(res.toArray)
  }

}

object RareMetal {
  val IK = Variant.InfoKey
  val FM = Annotation.Feature.values.zipWithIndex.toMap

}
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

package org.dizhang.seqspark.stat

import breeze.linalg.{DenseVector, shuffle}
import breeze.stats.distributions.Bernoulli
import org.dizhang.seqspark.assoc.Encode
import org.dizhang.seqspark.ds.SemiGroup.PairInt
import org.dizhang.seqspark.stat.HypoTest.NullModel

import scala.language.existentials

/**
  * resampling class
  *
  * residue permutation, or bootstrap
  *
  */

object Resampling {

  /**
    * the base class only resample the null model
    * convenient for the SmallSample SKAT test
    *  */
  @SerialVersionUID(7778770101L)
  final case class Base(nullModel: NullModel.Fitted) extends Resampling


  /**
    * This Simple class is for models that do not involve re-encoding genotype
    * */
  @SerialVersionUID(7778770301L)
  final case class Simple(refStatistic: Double, min: Int, max: Int,
                          nullModel: NullModel.Fitted, coding: Encode.Coding,
                          transformer: (NullModel.Fitted, Encode.Coding) => Double) extends Resampling {
    def makeNewStatistic(newNullModel: NullModel.Fitted): Double = {
      transformer(newNullModel, coding)
    }

    def pCount: PairInt = {
      var res = (0, 0)
      for (i <- 0 to max) {
        if (res._1 >= min)
          return res
        else {
          val newNullModel = makeNewNullModel
          val statistic = makeNewStatistic(newNullModel)
          res = PairInt.op(res, if (statistic > refStatistic) (1, 1) else (0, 1))
        }
      }
      res
    }
  }

}

@SerialVersionUID(7778770001L)
sealed trait Resampling extends HypoTest {
  def nullModel: NullModel.Fitted

  lazy val makeNewY: () => DenseVector[Double] = {
    if (nullModel.binary) {
      () => nullModel.estimates.map(p => if (new Bernoulli(p).draw()) 1.0 else 0.0)
    } else {
      () => nullModel.estimates + shuffle(nullModel.residuals)
    }
  }
  /** new null model is made based on newY,
    * which could be from permutated residuals (linear),
    * or from bootstrap (logistic)
    * */
  def makeNewNullModel: NullModel.Fitted = {
    val newY = makeNewY()
    val cols = nullModel.xs.cols
    NullModel(
      newY,
      nullModel.xs(::, 1 until cols),
      fit = true,
      binary = nullModel.binary
    ).asInstanceOf[NullModel.Fitted]
  }
}





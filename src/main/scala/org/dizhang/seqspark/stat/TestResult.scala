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


/**
 * Hold regression result
 */

object TestResult {
  def apply(est: Option[Double],
            std: Option[Double],
            sta: Double,
            pVa: Double): TestResult = {
    new TestResult {
      override def estimate: Option[Double] = est

      override def stdErr: Option[Double] = std

      override def pValue: Double = pVa

      override def statistic: Double = sta
    }
  }
}

trait TestResult {

  def estimate: Option[Double]

  def stdErr: Option[Double]

  def statistic: Double

  def pValue: Double

  override def toString = estimate match {
    case None => s",,$statistic,$pValue"
    case _ => s"$estimate,$stdErr,$statistic,$pValue"
  }

}


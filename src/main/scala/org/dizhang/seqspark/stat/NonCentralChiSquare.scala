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

import breeze.numerics.{exp, gammp, gammq, lgamma}
import breeze.stats.distributions.ChiSquared
/**
  * This cdf functions are copied and modified from:
  * https://github.com/quantscale/QuantScale/blob/master/
  * src/main/scala/quantscale/analytic/GammaDistribution.scala
  * */

object NonCentralChiSquare {
  val maxiter = 5000
  val errtol = 1e-8
  object Epsilon {
    val MACHINE_EPSILON = 2.2204460492503131e-016
    val MACHINE_EPSILON_SQRT = Math.sqrt(MACHINE_EPSILON)
    val MACHINE_EPSILON_FOURTH_ROOT = Math.sqrt(MACHINE_EPSILON_SQRT)
  }
  def gammaPDerivative(a: Double, x: Double): Double = {
    if (a <= 0) throw new RuntimeException("argument a must be > 0 but was " + a)
    if (x < 0) throw new RuntimeException("argument x must be >= 0 but was " + x)
    if (x == 0) {
      if (a > 1) return 0 else {
        if (a == 1) return 1 else throw new RuntimeException("Overflow")
      }
    }

    var f1 = 0.0
    if (x <= Epsilon.MACHINE_EPSILON_SQRT) {
      // Oh dear, have to use logs, should be free of cancellation errors though:
      f1 = math.exp(a * math.log(x) - x - exp(lgamma(a)))
    } else {
      // direct calculation, no danger of overflow as gamma(a) < 1/a
      // for small a.
      f1 = math.pow(x, a) * math.exp(-x) / lgamma(a)
    }
    f1 / x
  }

  @SerialVersionUID(7778620101L)
  case class NCCSDing(degrees: Double,
                      nonCentrality: Double) extends NonCentralChiSquare {
    def cdf(cutoff: Double): Double = {
      //
      // This is an implementation of:
      //
      // Algorithm AS 275:
      // Computing the Non-Central #2 Distribution Function
      // Cherng G. Ding
      // Applied Statistics, Vol. 41, No. 2. (1992), pp. 478-482.
      //
      // This uses a stable forward iteration to sum the
      // CDF, unfortunately this can not be used for large
      // values of the non-centrality parameter because:
      // * The first term may underfow to zero.
      // * We may need an extra-ordinary number of terms
      //   before we reach the first *significant* term.
      //

      if (cutoff > degrees + nonCentrality)
        return 1.0 - NCCSBentonKrishnamoorthy(degrees, nonCentrality).cdfq(cutoff)

      val x = cutoff
      val f = degrees
      val theta = nonCentrality
      // Special case:
      if (x < Epsilon.MACHINE_EPSILON)
        return 0.0
      var tk = gammaPDerivative(f / 2 + 1, x / 2)
      val lambda = theta / 2
      var vk = math.exp(-lambda)
      var uk = vk
      var sum = tk * vk
      if (sum == 0)
        return sum
      var lterm, term = 0.0
      var i = 1
      var isFinished = false
      while (i <= maxiter && !isFinished) {
        tk = tk * x / (f + 2 * i)
        uk = uk * lambda / i
        vk = vk + uk
        lterm = term
        term = vk * tk
        sum += term
        if ((math.abs(term / sum) < errtol) && (term <= lterm)) {
          isFinished = true
        }
        i += 1
      }
      //Error check:
      if (i >= maxiter)
        throw new RuntimeException(
          "Series did not converge, closest value was " + sum)
      sum
    }
  }
  @SerialVersionUID(7778620201L)
  case class NCCSBentonKrishnamoorthy(degrees: Double,
                                      nonCentrality: Double) extends NonCentralChiSquare {
    def cdfq(cutoff: Double): Double = {

      val x = cutoff
      val f = degrees
      val theta = nonCentrality

      // Special case:
      if (x == 0)
        return 1

      //
      // Initialize the variables we'll be using:
      //
      val lambda = theta / 2
      val del = f / 2
      val y = x / 2
      var sum = 0.0
      //
      // k is the starting location for iteration, we'll
      // move both forwards and backwards from this point.
      // k is chosen as the peek of the Poisson weights, which
      // will occur *before* the largest term.
      //
      val k: Int = math.round(lambda).toInt
      // Forwards and backwards Poisson weights:
      var poisf = gammaPDerivative(1.0 + k, lambda)
      var poisb = poisf * k / lambda
      // Initial forwards central chi squared term:
      var gamf = gammq(del + k, y)
      // Forwards and backwards recursion terms on the central chi squared:
      var xtermf = gammaPDerivative(del + 1 + k, y)
      var xtermb = xtermf * (del + k) / y
      // Initial backwards central chi squared term:
      var gamb = gamf - xtermb

      //
      // Forwards iteration first, this is the
      // stable direction for the gamma function
      // recurrences:
      //
      var i = k
      var isBreak = false
      while ((i - k) < maxiter && !isBreak) {
        val term = poisf * gamf
        sum += term
        poisf *= lambda / (i + 1)
        gamf += xtermf
        xtermf *= y / (del + i + 1)
        if (((sum == 0) || (math.abs(term / sum) < errtol)) && (term >= poisf * gamf))
          isBreak = true
        i += 1
      }
      //Error check:
      if ((i - k) >= maxiter)
        throw new RuntimeException(
          "Series did not converge, closest value was " + sum);
      //
      // Now backwards iteration: the gamma
      // function recurrences are unstable in this
      // direction, we rely on the terms deminishing in size
      // faster than we introduce cancellation errors.
      // For this reason it's very important that we start
      // *before* the largest term so that backwards iteration
      // is strictly converging.
      //
      i = k - 1
      isBreak = false
      while (i >= 0 && !isBreak) {
        val term = poisb * gamb
        sum += term
        poisb *= i / lambda
        xtermb *= (del + i) / y
        gamb -= xtermb
        if ((sum == 0) || (math.abs(term / sum) < errtol))
          isBreak = true
        i -= 1
      }

      sum
    }
    def cdf(cutoff: Double): Double = {
      //
      // This is taken more or less directly from:
      //
      // Computing discrete mixtures of continuous
      // distributions: noncentral chisquare, noncentral t
      // and the distribution of the square of the sample
      // multiple correlation coeficient.
      // D. Benton, K. Krishnamoorthy.
      // Computational Statistics & Data Analysis 43 (2003) 249 - 267
      //
      // We're summing a Poisson weighting term multiplied by
      // a central chi squared distribution.
      //
      // Special case:
      if (cutoff > degrees + nonCentrality)
        return 1.0 - cdfq(cutoff)

      val y = cutoff
      val n = degrees
      val lambda = nonCentrality
      if (y < Epsilon.MACHINE_EPSILON)
        return 0
      var errorf, errorb = 0.0

      val x = y / 2
      val del = lambda / 2
      //
      // Starting location for the iteration, we'll iterate
      // both forwards and backwards from this point.  The
      // location chosen is the maximum of the Poisson weight
      // function, which ocurrs *after* the largest term in the
      // sum.
      //
      val k: Int = math.round(del).toInt
      val a = n / 2 + k
      // Central chi squared term for forward iteration:
      var gamkf = gammp(a, x); //gammaP?

      if (lambda == 0)
        return gamkf
      // Central chi squared term for backward iteration:
      var gamkb = gamkf
      // Forwards Poisson weight:
      var poiskf = gammp(k + 1.0, del)
      // Backwards Poisson weight:
      var poiskb = poiskf
      // Forwards gamma function recursion term:
      var xtermf = gammp(a, x)
      // Backwards gamma function recursion term:
      var xtermb = xtermf * x / a
      var sum = poiskf * gamkf
      if (sum == 0)
        return sum
      var i = 1
      //
      // Backwards recursion first, this is the stable
      // direction for gamma function recurrences:
      //
      var isBreak = false
      while (i <= k && !isBreak) {
        xtermb *= (a - i + 1) / x
        gamkb += xtermb
        poiskb = poiskb * (k - i + 1) / del
        errorf = errorb
        errorb = gamkb * poiskb
        sum += errorb
        if ((math.abs(errorb / sum) < errtol) && (errorb <= errorf))
          isBreak = true
        i += 1
      }
      i = 1
      //
      // Now forwards recursion, the gamma function
      // recurrence relation is unstable in this direction,
      // so we rely on the magnitude of successive terms
      // decreasing faster than we introduce cancellation error.
      // For this reason it's vital that k is chosen to be *after*
      // the largest term, so that successive forward iterations
      // are strictly (and rapidly) converging.
      //
      do {
        xtermf = xtermf * x / (a + i - 1)
        gamkf = gamkf - xtermf
        poiskf = poiskf * del / (k + i)
        errorf = poiskf * gamkf
        sum += errorf
        i += 1
      } while ((math.abs(errorf / sum) > errtol) && (i < maxiter))

      //Error check:
      if (i >= maxiter)
        throw new RuntimeException(
          "Series did not converge, closest value was " + sum)

      sum
    }
  }
  @SerialVersionUID(7778620301L)
  case class CCS(degrees: Double) extends NonCentralChiSquare {
    def nonCentrality = 0.0
    private lazy val ccs = new ChiSquared(degrees)
    def cdf(cutoff: Double): Double = {
      ccs.cdf(cutoff)
    }
  }

  def apply(degrees: Double,
            nonCentrality: Double): NonCentralChiSquare = {
    if (nonCentrality == 0.0) {
      CCS(degrees)
    } else if (degrees >= 200) {
      NCCSBentonKrishnamoorthy(degrees, nonCentrality)
    } else {
      NCCSDing(degrees, nonCentrality)
    }
  }

}

@SerialVersionUID(7778620001L)
trait NonCentralChiSquare extends Serializable {
  def degrees: Double
  def nonCentrality: Double
  def cdf(cutoff: Double): Double
}
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

package org.dizhang.seqspark.numerics

import breeze.linalg.{DenseVector, max}
import Integrate._
import breeze.numerics.abs
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks._

/**
  * Created by zhangdi on 12/13/16.
  */
object Qags {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  /**
  case class Result1(value: Double, //the approximated integral
                    absErr: Double, //absolute error
                    nEval: Int, //number of integrand evaluations
                    iEr: Int, //0: normal, 1: maximum subdivisions achieved, 2: round off error
                              //3: bad integrand behaviour, 4: divergent 5: divergent 6: invalid input
                    aList: Array[Double], //left end points
                    bList: Array[Double], //right end points
                    rList: Array[Double], //approximated integrals
                    eList: Array[Double], //absolute errors
                    iOrd: Array[Int], //
                    last: Int //number of sub-intervals produced
                   )
  */

  case class Result() {
    /** use negative init values to indicate nothing done */
    var value: Double = -1.0
    var abserr: Double = -1.0
    var nEval: Int = -1
    var iEr: Int = -1 //exit code
    var nSub: Int = -1

    override def toString: String =
      s"""
        |value: $value
        |abserr: $abserr
        |neval: $nEval
        |ier: $iEr
        |nsub: $nSub
      """.stripMargin
  }

  case class QagsError(message: String = null,
                       cause: Throwable = null)
    extends RuntimeException(defaultMessage(message, cause), cause)
  private def defaultMessage(message: String, cause: Throwable) = {
    if (message != null) message
    else if (cause != null) cause.toString
    else null
  }

  private def isPositive(result: Double, resabs: Double): Boolean = {
    abs(result) >= (1 - 50 * EPSILON) * resabs
  }
  private def subInterval_tooSmall(a1: Double, a2: Double, b2: Double): Boolean = {
    val tmp = (1 + 100 * EPSILON) * (abs(a2) + 1000 * MINVALUE)
    //logger.debug(s"a1: $a1 a2: $a2 b1: $b2 x1: ${1 + 100 * EPSILON} x2: ${abs(a2) + 1000 * MINVALUE} tmp: $tmp")
    abs(a1) <= tmp && abs(b2) <= tmp
  }

  def apply(f: DenseVector[Double] => DenseVector[Double],
            a: Double, b: Double,
            epsAbs: Double, epsRel: Double,
            limit: Int,
            memory: Memory,
            result: Result): Int = {
    /** initiation*/
    var area: Double = 0.0
    var errSum: Double = 0.0
    var resExt: Double = 0.0
    var errExt: Double = 0.0

    var (result0, absErr0, resAbs0, resAsc0) = (0.0, 0.0, 0.0, 0.0)
    var tolerance = 0.0
    var ertest = 0.0
    var error_over_large_intervals = 0.0
    var (reseps, abseps, correc) = (0.0, 0.0, 0.0)
    var ktmin: Int = 0
    var (roundoff_type1, roundoff_type2, roundoff_type3) = (0, 0, 0)
    var (error_type, error_type2) = (0, 0)

    var iteration: Int = 0

    var positive_integrand: Boolean = true
    var extrapolate: Boolean = false
    var disallow_extrapolation: Boolean = false

    val table = Qelg.Extrapolation()

    memory.init(a, b)

    if (limit > memory.limit) {
      throw QagsError("iteration limit exceeds available memory")
    }

    if (epsAbs <= 0 && (epsRel < 50 * EPSILON || epsRel < 0.5e-28)) {
      throw QagsError("tolerance cannot be achieved with given epsabs and epsrel")
    }

    /** first approximation to the integral */

    Qk21(f, a, b) match {
      case (x1, x2, x3, x4) =>
        result0 = x1
        absErr0 = x2
        resAbs0 = x3
        resAsc0 = x4
    }
    memory.pushFirst(result0, absErr0)
    tolerance = max(epsAbs, epsRel * abs(result0))

    if (absErr0 <= 100 * EPSILON * resAbs0 && absErr0 > tolerance) {
      result.value = result0
      result.abserr = absErr0
      throw QagsError("cannot reach tolerance because of roundoff error on first attempt")
    } else if ((absErr0 <= tolerance && absErr0 != resAsc0) || absErr0 == 0.0) {
      result.value = result0
      result.abserr = absErr0
      return 1
    } else if (limit == 1) {
      result.value = result0
      result.abserr = absErr0
      throw QagsError("a maximum of one iteration was insufficient")
    }

    table.append(result0)

    area = result0
    errSum = absErr0

    resExt = result0
    errExt = MAXVALUE

    positive_integrand = isPositive(result0, resAbs0)

    iteration = 1

    var goto_compute: Boolean = false
    var goto_return_error: Boolean = false
    breakable{
      do {
        var skip = false
        var current_level: Int = 0
        var (a1, b1, a2, b2,
        a_i, b_i, r_i, e_i,
        area1, area2, area12,
        error1, error2, error12,
        resasc1, resasc2,
        resabs1, resabs2,
        last_e_i) =
          (0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0.0,
            0.0, 0.0, 0.0,
            0.0, 0.0,
            0.0, 0.0,
            0.0)
        memory.getMax match {
          case (x1, x2, x3, x4) =>
            a_i = x1
            b_i = x2
            r_i = x3
            e_i = x4
        }

        current_level = memory.level(memory.maxErr) + 1

        a1 = a_i
        b1 = 0.5 * (a_i + b_i)
        a2 = b1
        b2 = b_i

        iteration += 1

        Qk21(f, a1, b1) match {
          case (x1, x2, x3, x4) =>
            area1 = x1
            error1 = x2
            resabs1 = x3
            resasc1 = x4
        }

        Qk21(f, a2, b2) match {
          case (x1, x2, x3, x4) =>
            area2 = x1
            error2 = x2
            resabs2 = x3
            resasc2 = x4
        }

        area12 = area1 + area2
        error12 = error1 + error2
        last_e_i = e_i

        errSum += error12 - e_i
        //logger.debug(s"current errsum: $errSum error12: $error12 e_i: $e_i")
        area += area12 - r_i

        tolerance = max(epsAbs, epsRel * abs(area))

        if (resasc1 != error1 && resasc2 != error2) {
          val delta = r_i - area12

          if (abs(delta) <= 1e-5 * abs(area12) && error12 >= 0.99 * e_i) {
            if (extrapolate) {
              roundoff_type1 += 1
            } else {
              roundoff_type2 += 1
            }
          }
          if (iteration > 10 && error12 > e_i) {
            roundoff_type3 += 1
          }
        }

        if (roundoff_type1 + roundoff_type2 >= 10 || roundoff_type3 >= 20) {
          error_type = 2
        }
        if (roundoff_type2 >= 5) {
          error_type2 = 1
        }
        if (subInterval_tooSmall(a1, a2, b2)) {
          error_type = 4
        }

        memory.update(a1, b1, area1, error1, a2, b2, area2, error2)

        if (errSum <= tolerance) {
          logger.trace("accurary achieved, go compute result")
          goto_compute = true
          break()
        }
        if (error_type != 0) {
          logger.trace(s"error_type $error_type, break")
          break()
        }

        if (iteration >= limit - 1) {
          logger.trace("max iterations, break")
          error_type = 1
          break()
        }

        if (iteration == 2) {
          logger.trace("set up variables on first iteration")
          error_over_large_intervals = errSum
          ertest = tolerance
          table.append(area)
          skip = true
        }

        if (! skip && disallow_extrapolation) {
          skip = true
        }

        if (! skip) {
          error_over_large_intervals += -last_e_i
        }

        if (!skip && current_level < memory.maxLevel) {
          error_over_large_intervals += error2
        }

        if (!skip && !extrapolate) {
          if (memory.large_interval) {

            skip = true
          }
          extrapolate = true
          memory.nrMax = 1
        }
        if (!skip && (error_type2 != 0) && error_over_large_intervals > ertest) {
          if (memory.increase_nrmax) {
            skip = true
          }
        }
        if (!skip) {
          table.append(area)
          //logger.debug(s"perform extrapolation")
          Qelg(table) match {
            case (x1, x2) =>
              reseps = x1
              abseps = x2
          }
          //logger.debug(s"performed extrapolation, table after Qelg: $table")
          ktmin += 1

          if (ktmin > 5 && errExt < 0.001 * errSum) {
            error_type = 5
          }
          if (abseps < errExt) {
            ktmin = 0
            errExt = abseps
            resExt = reseps
            correc = error_over_large_intervals
            ertest = max(epsAbs, epsRel * abs(reseps))
            if (errExt <= ertest) {
              logger.trace(s"errExt: $errExt ertest: $ertest abseps: $abseps , break")
              break()
            }
          }

          if (table.n == 1) {
            disallow_extrapolation = true
          }
          if (error_type == 5) {
            logger.trace("raw error_type 5, break")
            break()
          }
          memory.reset_nrmax()
          extrapolate = false
          error_over_large_intervals = errSum
        }
      } while (iteration < limit)
    }

    breakable{
      if (goto_return_error || goto_compute) {
        logger.trace("force goto compute or return error")
        break()
      }
      result.value = resExt
      result.abserr = errExt

      if (errExt == MAXVALUE) {
        goto_compute = true
        break()
      }
      if (error_type != 0 || error_type2 != 0) {
        if (error_type2 != 0) {
          errExt += correc
        }
        if (error_type == 0) {
          error_type = 3
        }
        if (resExt != 0.0 && area != 0.0) {
          if (errExt / abs(resExt) > errSum/abs(area)) {
            goto_compute = true
            break()
          }
        } else if (errExt > errSum) {
          goto_compute = true
          break()
        } else if (area == 0.0) {
          goto_return_error = true
          break()
        }
      }
      val max_area = max(abs(resExt), abs(area))
      if (! positive_integrand && max_area < 0.01 * resAbs0) {
        goto_return_error = true
        break()
      }
      val ratio = resExt/area
      if (ratio < 0.01 || ratio > 100.0 || errSum > abs(area)) {
        error_type = 6
      }
      goto_return_error = true
    }

    result.iEr = error_type
    result.nEval = iteration * 2 + 1
    result.nSub = memory.size

    if (! goto_return_error) {
      result.value = memory.sumResult
      result.abserr = errSum
      0
    }

    if (error_type > 2) {
      error_type -= 1
    }

    error_type match {
      case 0 =>
        0
      case 1 =>
        throw QagsError("number of iterations was insufficient")
      case 2 =>
        throw QagsError("cannot reach tolerance because of roundoff error")
      case 3 =>
        throw QagsError("bad integrand behavior found in the integration interval")
      case 4 =>
        throw QagsError("roundoff error detected in the extrapolation table")
      case 5 =>
        throw QagsError("integral is divergent, or slowly convergent")
      case _ =>
        throw QagsError("could not integrate function")
    }
    error_type
  }
}

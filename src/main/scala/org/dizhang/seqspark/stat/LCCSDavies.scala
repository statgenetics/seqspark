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

import breeze.linalg.{DenseVector, max, min, sum}
import breeze.numerics._
import breeze.numerics.constants.Pi
import org.dizhang.seqspark.stat.LCCSDavies._
import org.dizhang.seqspark.stat.{LinearCombinationChiSquare => LCCS}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Based on Davies' method (1980)
  * The code is quite old, containing a lot of
  * global variables and side effects
  * Be careful!
  *
  * This is NOT a full implementation of the original method
  * in some functions, non-centralities == 0 is assumed.
  * Maybe I will solve this later.
  *
  */
object LCCSDavies {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  case class DaviesError(message: String = null, cause: Throwable = null)
    extends RuntimeException(defaultMessage(message, cause), cause)

  private def defaultMessage(message: String, cause: Throwable): String = {
    if (message != null) message
    else if (cause != null) cause.toString
    else null
  }

  private val limit: Double = 1e4
  private val errorTolerance = 1e-6
  private val ln28 = math.log(2)/8

  def exp1(x: Double): Double = {
    if (x < -50.0) 0.0 else exp(x)
  }

  implicit class RichDouble(val r: Double) extends AnyVal {
    def square = r * r
    def cube = r * r * r
  }

  def log1(x: Double, first: Boolean): Double = {

    if (abs(x) > 0.1) {
      val res = if (first) log(1.0 + x) else log(1.0 + x) - x
      //println(s"log1: x: $x > 0.1 first: $first result: $res")
      res
    } else {
      var y = x / (2.0 + x)
      var term = 2.0 * y.cube
      var k = 3.0
      var s = (if (first) 2.0 else -x) * y
      y = y.square
      var s1 = s + term/k
      while (s != s1) {
        k += 2.0
        term *= y
        s = s1
        s1 = s + term/k
      }
      s
    }
  }
  case class CDFResult(pvalue: Double,
                       ifault: Int,
                       trace: Array[Double]) extends LCCS.CDF {
    override def toString =
      """Pvalue:   %10f
        |Ifault:   %10d
        |                       1: required accuracy not achieved
        |                       2: round-off error possibly significant
        |                       3: invalid parameters
        |                       4: unable to locate integration parameters
        |                       5: out of memory
        |Trace0:   %10f   absolute sum
        |Trace1:   %10f   total number of integration terms
        |Trace2:   %10f   number of integrations
        |Trace3:   %10f   integration interval in final integration
        |Trace4:   %10f   truncation point in initial integration
        |Trace5:   %10f   s.d. of initial convergence factor
        |Trace6:   %10f   cycles to locate integration parameters
      """.stripMargin.format(pvalue, ifault, trace(0), trace(1), trace(2),
        trace(3), trace(4), trace(5), trace(6))
  }
  @SerialVersionUID(7778530101L)
  case class Simple(lambda: DenseVector[Double]) extends LCCSDavies {
    /** the original code is more versatile,
      * we set some variables to constants here
      * note that, it is easy to put these variables to
      * the class parameter to get the original version */
    val nonCentrality = DenseVector.fill(size)(0.0)
    val degreeOfFreedom = DenseVector.fill(size)(1.0)
    val sigma = 0.0
  }
}

@SerialVersionUID(7778530001L)
trait LCCSDavies extends LCCS {
  def sigma: Double
  var sigsq = sigma.square
  def lmax = max(lambda)
  def lmin = min(lambda.toArray :+ 0.0)
  lazy val th: Array[Int] = {
    /** the order of abs(lambda)
      * with the largest labeled 0 */
    val absLambda = abs(lambda).toArray
    absLambda.zipWithIndex.sortBy(_._1).map(_._2)
      .reverse.zipWithIndex.sortBy(_._1).map(_._2)
  }

  def errbd(u: Double)(implicit counter: () => Int): (Double, Double) = {
    /** find bound on tail probability using mgf, cutoff point returned to _._2 */
    //print(s"errdb u: $u cx: 0.0")
    counter()
    val xconst_init = u * sigsq
    val sum1_init = u * xconst_init
    val u2 = 2.0 * u
    val x = u2 * lambda
    val y = DenseVector.fill(size)(1.0) - x
    val xconst = xconst_init + sum(lambda *:* ((nonCentrality /:/ y) + degreeOfFreedom) /:/ y)
    val sum1 = sum1_init + sum(
      (nonCentrality *:* pow(x /:/ y, 2)) +
        degreeOfFreedom *:* ((pow(x, 2) /:/ y) + x.map(e => log1(-e, first = false)))
    )
    val res = (exp1(-0.5 * sum1), xconst)
    //println(s" res: ${res._1} newcx: ${res._2}")
    res
  }

  def ctff(accx: Double, upn: Double)(implicit counter: () => Int): (Double, Double) = {
    /** find ctff so that p(qf > ctff) < accx
      * if (upn > 0, p(qf < ctff) < accx otherwise)*/
    //println(s"ctff--start accx: $accx upn: $upn")
    var u2 = upn
    var u1 = 0.0
    var c1 = meanLambda
    val rb = 2.0 * (if (u2 > 0.0) lmax else lmin)
    var u = u2/(1.0 + u2 * rb)
    val errc2 = Array[Double](accx + 1, 0.0)
    var xconst = 0.0
    //println(s"ctff--before loop| u2: $u2 rb: $rb")
    while ({val tmp = errbd(u)
      errc2(0) = tmp._1
      errc2(1) = tmp._2
      errc2(0) > accx}) {
      u1 = u2
      c1 = errc2(1)
      u2 = 2.0 * u2
      u = u2/(1.0 + u2 * rb)
    }
    //println(s"u: $u c1: $c1 c2: ${errc2(1)}")
    u = (c1 - meanLambda)/(errc2(1) - meanLambda)
    while (u < 0.9) {
      u = (u1 + u2)/2.0
      val tmp = errbd(u/(1.0 + u * rb))
      errc2(0) = tmp._1
      xconst = tmp._2
      if (errc2(0) > accx) {
        u1 = u
        c1 = xconst
      } else {
        u2 = u
        errc2(1) = xconst
      }
      u = (c1 - meanLambda)/(errc2(1) - meanLambda)
    }
    //println(s"ctff--end newupn: $u2 res: ${errc2(1)}")
    (errc2(1), u2)
  }

  def truncation(u: Double,
                 tausq: Double)(implicit counter: () => Int): Double = {
    /** bound integration error due to truncation at u */
    counter()
    //logger.debug(s"truncation input -- u: $u tausq: $tausq")
    val sum2 = (sigsq + tausq) * u.square
    val prod1_init = 2.0 * sum2
    val u2 = 2.0 * u
    val tmpX = lambda.map(l => (l * u2).square)
    val sum1 = 0.5 * (nonCentrality dot tmpX.map(x => x/(1.0 + x)))
    val prod2_tmp = degreeOfFreedom dot tmpX.map(x => if (x > 1.0) log(x) else 0.0)
    val prod3_tmp = degreeOfFreedom dot tmpX.map(x => if (x > 1.0) log1(x, first = true) else 0.0)
    val prod1 = prod1_init + (
      degreeOfFreedom dot tmpX.map(x => if (x > 1.0) 0.0 else log1(x, first = true))
      )
    val totalDoF = degreeOfFreedom dot tmpX.map(x => if (x > 1.0) 1.0 else 0.0)
    val prod2 = prod1 + prod2_tmp
    val prod3 = prod1 + prod3_tmp
    //logger.debug(s"tmpX: ${tmpX.toArray.mkString("\n")}")
    //logger.debug(s"log1X: ${tmpX.map(x => log1(x, true)).toArray.mkString("\n")}")
    //logger.debug(s"truncation tmps -- prod1: $prod1 prod2: $prod2 prod3: $prod3 sum1: $sum1")
    val x = exp1(-sum1 - 0.25 * prod2)/Pi
    val y = exp1(-sum1 - 0.25 * prod3)/Pi
    val err1 = if (totalDoF == 0.0) 1.0 else x * 2.0/totalDoF
    val err2 = if (prod3 > 1.0) 2.5 * y else 1.0
    //logger.debug(s"err1: $err1 err2: $err2")
    val x2 = 0.5 * sum2
    val err3 = min(err1, err2)
    val err4 = if (x2 <= y) 1.0 else y/x2
    val res = min(err3, err4)
    //logger.debug(s"truncation output -- res: $res")
    res
  }

  def findu(utx: Double, accx: Double)(implicit counter: () => Int): Double = {
    /** find u such that truncation(u) < accx
      * and truncation(u/1.2) > accx
      * */
    //logger.debug(s"findu -- utx: $utx accx: $accx")
    val divis = Array(2.0, 1.4, 1.2, 1.1)
    var ut: Double = utx
    var u = ut/4.0
    if (truncation(u, 0.0) > accx) {
      //logger.debug("1")
      u = ut
      while (truncation(u, 0.0) > accx) {
        ut *= 4.0
        u = ut
      }
    } else {
      //logger.debug("2")
      ut = u
      u = u/4.0
      while (truncation(u, 0.0) <= accx) {
        ut = u
        u = u/4.0
        //logger.debug(s"2 u: $u ut:$ut")
      }
    }
    //logger.debug(s"ut: $ut")
    for (i <- divis.indices) {
      u = ut/divis(i)
      if (truncation(u, 0.0) <= accx) ut = u
      //logger.debug(s"u: $u ut:$ut")
    }
    //logger.debug(s" ut: $ut")
    ut
  }

  def integrate(cutoff: Double, intl: Double, ersm: Double,
                nterm: Int, interv: Double, tausq: Double, mainx: Boolean): (Double, Double) = {
    /**
    *printf("%6s %13s %13s %s %6s %13s %13s %6s\n",
      *"phase", "intl", "ersm", "cutoff",
      *"nterm", "interv", "tausq", "mainx")
    *printf("%6s %13.10f %13.10f %6.4f %6d %13.10f %13.10f %6s\n",
      *"begin", intl, ersm, cutoff,
      *nterm, interv, tausq, mainx.toString)
      */
    var (inpi, u, sum1, sum2, sum3, x) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    val res = Array[Double](intl, ersm)
    inpi = interv / Pi
    for (k <- (0 to nterm).reverse) {
      u = (k + 0.5) * interv
      sum1 = -2.0 * u * cutoff
      sum2 = abs(sum1)
      sum3 = -0.5 * sigsq * u.square
      val tmpX = 2.0 * u * lambda
      val tmpY = pow(tmpX, 2)
      sum3 += -0.25 * sum(degreeOfFreedom *:* tmpY.map(y => log1(y, true)))
      val tmpY2 = (nonCentrality *:* tmpX) /:/ tmpY.map(1.0 + _)
      val tmpZ = (degreeOfFreedom *:* atan(tmpX)) + tmpY2
      sum1 += sum(tmpZ)
      sum2 += abs(sum(tmpZ))
      sum3 += -0.5 * (tmpX dot tmpY2)
      x = inpi * exp1(sum3)/u
      if (!mainx) {
        x *= 1.0 - exp1(-0.5 * tausq * u.square)
      }
      sum1 = sin(0.5 * sum1) * x
      sum2 *= 0.5 * x
      res(0) += sum1
      res(1) += sum2
    }
    /**
    *printf("%6s %13.10f %13.10f %6.4f %6d %13.10f %13.10f %6s\n",
      *"end", res(0), res(1), cutoff,
      *nterm, interv, tausq, mainx.toString)
      */
    (res(0), res(1))
  }

  def cfe(x: Double)(implicit counter: () => Int, toggler: () => Boolean): Double = {
    /** coef of tausq in error when convergence factor
      * of exp1(-0.5*tausq*u.square) is used when df is
      * evaluated at x */
    //print(s"cfe x: $x ")
    counter()
    var axl = abs(x)
    val sxl = if (x > 0.0) 1.0 else -1.0
    var sum1 = 0.0
    var continue = true
    for (i <- (0 until size).reverse; if continue) {
      val t = th(i)
      if (lambda(t) * sxl > 0.0) {
        val lj = abs(lambda(t))
        val axl1 = axl - lj * (degreeOfFreedom(t) + nonCentrality(t))
        val axl2 = lj/ln28
        if (axl1 > axl2) {
          axl = axl1
        } else {
          if (axl > axl2) axl = axl2
          sum1 = (axl - axl1)/lj
          sum1 += sum(for (k <- 0 until i) yield degreeOfFreedom(th(k)) + nonCentrality(th(k)))
          continue = false
        }
      }
    }
    //print(s"sum1: $sum1 axl: $axl ")
    val res =
      if (sum1 > 100.0) {
        toggler()
        1.0
      } else {
        pow(2.0, sum1/4.0)/(Pi * axl.square)
      }
    //println(s"res: $res")
    res
  }

  def cdf(cutoff: Double): LCCS.CDF = {
    /** Use closure to fake global counter
      * cnt is the free variable here
      * we will implicitly pass plusOne around
      * */
    var cnt = 0
    def counter(i: Int): () => Int = {
      () => {cnt += i; cnt}
    }
    implicit val plusOne = counter(1)
    var fail = false
    def toggler(b: Boolean): () => Boolean = {
      () => {fail = b; b}
    }
    implicit val setTrue = toggler(true)

    /** re-init sigsq */
    sigsq = sigma.square

    val trace: Array[Double] = Array.fill(7)(0.0)
    var ifault: Int = 0
    val rats = Array(1, 2, 4, 8)
    var qfval = -1.0
    var (intl, ersm) = (0.0, 0.0)
    val sdLambda: Double = sqrt(pow(lambda, 2) dot DenseVector.fill(size)(2.0))
    var xlim = limit
    //logger.debug(s"sd: $sdLambda")
    if (sdLambda == 0.0) {
      return if (cutoff > 0.0) CDFResult(1.0, ifault, trace) else CDFResult(0.0, ifault, trace)
    }
    val almx = max(lmax, -lmin)
    /** starting values for findu, ctff */
    var utx = 16.0/sdLambda
    var up = 4.5/sdLambda
    var un = -up
    /** truncation point with no convergence factor */
    //logger.debug(s"utx before: $utx, tol: $errorTolerance")
    utx = findu(utx, 0.5 * errorTolerance)
    //logger.debug(s"utx: $utx")
    /** does convergence factor help? */
    if (cutoff != 0.0 && (almx > 0.07 * sdLambda)) {
      val tausq = 0.25 * errorTolerance / cfe(cutoff)
      if (fail) {
        fail = false
      } else if (truncation(utx, tausq) < 0.2 * errorTolerance) {
        sigsq += tausq
        //logger.debug(s"utx: $utx acc1: $errorTolerance")
        utx = findu(utx, 0.25 * errorTolerance)
        //logger.debug(s"utx: $utx")
        trace(5) = sqrt(tausq)
      }
    }
    trace(4) = utx
    var acc1 = 0.5 * errorTolerance
    var (xnt, xntm) = (0.0, 0.0)
    var continue = true
    var intv = 0.0
    /** l1 label in original source code, changed to while */
    while (continue) {
      val tmp1 = ctff(acc1, up)
      val d1 = tmp1._1 - cutoff
      up = tmp1._2
      //logger.debug(s"d1: $d1 up: $up")
      if (d1 < 0.0) {
        trace(6) = cnt
        return CDFResult(1.0, ifault, trace)
      }
      //logger.debug(s"acc1: $acc1 un: $un")
      val tmp2 = ctff(acc1, un)
      val d2 = cutoff - tmp2._1
      un = tmp2._2
      //logger.debug(s"d2: $d2 un: $un")
      if (d2 < 0.0) {
        trace(6) = cnt
        return CDFResult(0.0, ifault, trace)
      }
      intv = 2.0 * Pi / max(d1, d2)
      /** calculate number of terms required for main and auxillary integrations */
      xnt = utx/intv
      xntm = 3.0 / sqrt(acc1)
      //logger.debug(s"utx: $utx intv: $intv xnt: $xnt xntm: $xntm")
      if (xnt > xntm * 1.5) {
        /** parameters for auxillary integration */
        if (xntm > xlim) {
          ifault = 1
          trace(6) = cnt
          return CDFResult(qfval, ifault, trace)
        }
        val ntm = floor(xntm + 0.5).toInt
        val intv1 = utx/ntm
        val x = 2.0 * Pi / intv1
        if (x <= abs(cutoff)) {
          continue = false
        } else {
          /** calculate convergence factor */
          val tausq = 0.33 * acc1 / (1.1 * (cfe(cutoff - x) + cfe(cutoff + x)))
          if (fail) {
            continue = false
          } else {
            acc1 = 0.67 * acc1
            val tmp = integrate(cutoff, intl, ersm, ntm, intv1, tausq, mainx = false)
            intl = tmp._1
            ersm = tmp._2
            xlim = xlim - xntm
            sigsq = sigsq + tausq
            trace(2) += 1
            trace(1) += ntm + 1
            utx = findu(utx, 0.25 * acc1)
          }
        }
      } else {
        continue = false
      }
    }

    /** main integration */
    trace(3) = intv
    if (xnt > xlim) {
      ifault = 1
      return CDFResult(qfval, ifault, trace)
    }
    val nt = floor(xnt + 0.5).toInt
    val tmp = integrate(cutoff, intl, ersm, nt, intv, 0.0, mainx = true)
    intl = tmp._1
    ersm = tmp._2
    trace(2) += 1
    trace(1) += nt + 1
    qfval = 0.5 - intl
    trace(0) = ersm

    /** test whether round-off error could be significant allow for radix 8 or 16 machines */
    up = ersm
    val x = up + errorTolerance/10.0
    for (j <- (0 until 4).reverse) {
      if (rats(j) * x == rats(j) * up)
        ifault = 2
    }
    trace(6) = cnt
    CDFResult(qfval, ifault, trace)
  }
}

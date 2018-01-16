/*
 * Copyright 2018 Zhang Di
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

import org.dizhang.seqspark.util.QueryParser._

import scala.util.{Failure, Success, Try}
import scala.util.parsing.combinator.JavaTokenParsers
/**
  * Created by zhangdi on 11/23/16.
  */
class QueryParser extends JavaTokenParsers with Serializable {


  def expr: Parser[QueryExpr] = term~rep("[+-].r"~term) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, "+" ~ t2) => BiOp(Arith.Add, t1, t2)
      case (t1, "-" ~ t2) => BiOp(Arith.Sub, t1, t2)
    }
  }
  def term: Parser[QueryExpr] = factor~rep("[/*]".r ~ factor) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, "*" ~ t2) => BiOp(Arith.Mul, t1, t2)
      case (t1, "/" ~ t2) => BiOp(Arith.Div, t1, t2)
    }
  }
  def factor: Parser[QueryExpr] = ("("~expr~")" ^^ { case "("~x~")" => x }) | dbkey | inDb

  def dbkey: Parser[QueryExpr] = """\w+""".r~"."~"""\w+""".r ^^ {
    case db ~ _ ~ key => DbKey(db, key)
  }

  def inDb: Parser[QueryExpr] = """\w+""".r ^^ (name => Db(name))

  def parse(input: String): ParseResult[QueryExpr] = this.parseAll(expr, input)
}

object QueryParser {

  val dbExists: String = Constant.Variant.dbExists

  def parse(input: List[String]): QueryExpr = {
    if (input.isEmpty || input.forall(_.isEmpty))
      Empty
    else
      parse(input.reduce((a,b) => s"($a) and ($b)"))
  }

  def parse(input: String): QueryExpr = {
    if (input.isEmpty)
      Empty
    else
      new QueryParser().parse(input).get
  }

  def parse(input: Map[String, String]): Map[String, QueryExpr] = {
    input.map(p => p._1 -> parse(p._2))
  }

  object Arith extends Enumeration {
    val Add = Value("+")
    val Sub = Value("-")
    val Mul = Value("*")
    val Div = Value("/")
  }

  sealed trait QueryExpr

  case object Empty extends QueryExpr
  case class Db(db: String) extends QueryExpr
  case class DbKey(db: String, key: String) extends QueryExpr
  case class BiOp(op: Arith.Value, e1: QueryExpr, e2: QueryExpr) extends QueryExpr

  def dbKeys(qe: QueryExpr): Map[String, Set[String]] = {
    qe match {
      case Db(db) => Map(db -> Set(dbExists))
      case DbKey(db, key) => Map(db -> Set(key))
      case BiOp(_, e1, e2) => merge(dbKeys(e1), dbKeys(e2))
      case Empty => Map[String, Set[String]]()
    }
  }

  /** this function is used to get all dbs and keys */
  def dbKeys(qes: Iterable[QueryExpr]): Map[String, Set[String]] = {
    if (qes.isEmpty)
      Map[String, Set[String]]()
    else
      qes.map(qe => dbKeys(qe)).reduce((a, b) => merge(a, b))
  }

  def merge(m1: Map[String, Set[String]], m2: Map[String, Set[String]]): Map[String, Set[String]] = {
    m2 ++ (for ((k, v) <- m1) yield if (m2.contains(k)) k -> (v ++ m2(k)) else k -> v)
  }

  def dbs(qe: QueryExpr): Set[String] = dbKeys(qe).keys.toSet

  def dbs(qes: Iterable[QueryExpr]): Set[String] = dbKeys(qes).keys.toSet



  /**
    * evaluate the expression to a number
    * if no key was found, None
    * if not a number, NaN
    *   except that it is a db existance term, which will be evaluated to 1
    *   this allows db existance addable
    *  */
  def getNumeber(qe: QueryExpr)(info: Map[String, Map[String, String]]): Option[Double] = {
    val dblopt = info.map(p => p._1 -> p._2.map(q => q._1 -> q._2.toDblopt))

    def helper(qe: QueryExpr)(data: Map[String, Map[String, Option[Double]]]): Option[Double] = {
      val zero = Map[String, Option[Double]]()
      qe match {
        case Empty => None
        case Db(db) =>
          data.getOrElse(db, zero).getOrElse(dbExists, Some(0))
        case DbKey(db, key) =>
          data.getOrElse(db, zero).getOrElse(key, None)
        case BiOp(op, e1, e2) =>
          (helper(e1)(data), helper(e2)(data)) match  {
            case (Some(d1), Some(d2)) => op match {
              case Arith.Add => Some(d1 + d2)
              case Arith.Sub => Some(d1 - d2)
              case Arith.Mul => Some(d1 * d2)
              case Arith.Div => Some(d1 / d2)
            }
            case _ => None
          }
      }
    }
    helper(qe)(dblopt)
  }

  /**
    * evaluate the expression to a string
    * if no key was found, None
    *   otherwise get the value, if it is db existance, use the dbExists key
    * if it is a function, to try evaluate it to a number first
    * */
  def getString(qe: QueryExpr)(info: Map[String, Map[String, String]]): Option[String] = {
    val stropt: Map[String, Map[String, Option[String]]] =
      info.map(p => p._1 -> p._2.map(q => q._1 -> toStrOpt(q._2)))

    def helper(qe: QueryExpr)(data: Map[String, Map[String, Option[String]]]): Option[String] = {
      val zero = Map[String, Option[String]]()
      qe match {
        case Empty => None
        case Db(db) =>
          data.getOrElse(db, zero).getOrElse(dbExists, None)
        case DbKey(db, key) =>
          data.getOrElse(db, zero).getOrElse(key, None)
        case x => /** if the expr is a function, eval to number first */
          getNumeber(x)(info).map(d => if (d.isValidInt) d.toInt.toString else d.toString)
      }
    }
    helper(qe)(stropt)
  }

  /** put this outside the implicit class because of strange compiling errors */
  def toStrOpt(value: String): Option[String] = {
    if (value == "" || value == ".") None else Some(value)
  }

  implicit class stropt(val value: String) extends AnyVal {

    def toDblopt: Option[Double] = {
      if (value == "" || value == ".")
        None
      else if (value == "true")
        Some(1)
      else
        Try(value.toDouble) match {
          case Success(d) => Some(d)
          case Failure(_) => Some(Double.NaN)
        }
    }
  }

  /**
    * evaluate a map
    *   the result can be serialized to VCF INFO field
    * */
  def eval(qes: Map[String, QueryExpr])(info: Map[String, Map[String, String]]): Map[String, String] = {
    qes.map(p => p._1 -> getString(p._2)(info))
      .filter(_._2.isDefined)
      .map(p => p._1 -> p._2.get)
  }

}
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

import org.dizhang.seqspark.util.GroupParser._

import scala.util.parsing.combinator.JavaTokenParsers

class GroupParser extends JavaTokenParsers {

  def expr: Parser[GroupExpr] = term ~ rep("and" ~ term) ^^ {
    case t ~ ts => ts.foldLeft(t){
      case (a, _ ~ b) => And(a, b)
    }
  }

  def term: Parser[GroupExpr] = """\w+""".r~"."~"""\w+""".r ^^ {
    case "samples" ~ _ ~ key => SampleGroup(key)
    case "variants" ~ _ ~ key => VariantGroup(key)
    case _ => Empty
  } | samples

  def samples: Parser[GroupExpr] = "samples" ^^ {_ => Samples}

  def parse(input: String): ParseResult[GroupExpr] = this.parseAll(expr, input)

}

object GroupParser {

  def parse(input: String): GroupExpr = {
    if (input.isEmpty) Empty else new GroupParser().parse(input).get
  }

  /**
  def eval(grpExpr: GroupExpr)
          (basis: Map[String, Map[String, LogicalParser]]): Map[String, Map[String, LogicalParser]] = {

  }
  */

  def toMap(grpExpr: GroupExpr): Map[String, Set[String]] = {
    grpExpr match {
      case Empty => Map[String, Set[String]]()
      case Samples => Map("bySamples" -> Set("true"))
      case SampleGroup(k) => Map("samples" -> Set(k))
      case VariantGroup(k) => Map("variants" -> Set(k))
      case And(g1, g2) =>
        val m1 = toMap(g1)
        val m2 = toMap(g2)
        m1 ++ (for ((k, v) <- m2) yield if (m1.contains(k)) k -> (m1(k) ++ v) else k -> v)
    }
  }

  sealed trait GroupExpr
  case object Empty extends GroupExpr
  case object Samples extends GroupExpr
  case class SampleGroup(name: String) extends GroupExpr
  case class VariantGroup(name: String) extends GroupExpr
  case class And(grp1: GroupExpr, grp2: GroupExpr) extends GroupExpr

}
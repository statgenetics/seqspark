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

import org.dizhang.seqspark.util.LogicalParser._

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Created by zhangdi on 11/23/16.
  */
class LogicalParser extends JavaTokenParsers with Serializable {
  def expr: Parser[LogExpr] = term~rep("or"~term) ^^ {
    case f1 ~ fs => (f1 /: fs)((x, y) => OR(x, y._2))
  }
  def term: Parser[LogExpr] = factor~rep("and"~factor) ^^ {
    case f1 ~ fs => (f1 /: fs)((x, y) => AND(x, y._2))
  }
  def factor: Parser[LogExpr] = comparison | ("("~expr~")" ^^ { case "("~x~")" => x })
  def comparison: Parser[LogExpr] = stringComparison | numberComparison | existence
  def stringComparison: Parser[LogExpr] =
    """[a-zA-Z_](\w|\.)*""".r~("=="|"!=")~stringLiteral ^^ {
      case name~"=="~value => SEQ(name, value.substring(1,value.length - 1))
      case name~"!="~value => SNE(name, value.substring(1,value.length - 1))
    }
  def numberComparison: Parser[LogExpr] =
    """[a-zA-Z_](\w|\.)*""".r~(">="|"<="|"=="|"!="|">"|"<")~floatingPointNumber ^^ {
      case name~">"~value => GT(name, value.toDouble)
      case name~">="~value => GE(name, value.toDouble)
      case name~"<"~value => LT(name, value.toDouble)
      case name~"<="~value => LE(name, value.toDouble)
      case name~"=="~value => EQ(name, value.toDouble)
      case name~"!="~value => NE(name, value.toDouble)
    }
  def existence: Parser[LogExpr] =
    opt("!"|"not") ~ """[a-zA-Z_]\w*""".r ^^ {
      case None ~ name => EX(name)
      case Some(_) ~ name => Not(EX(name))
    }
  def parse(input: String): ParseResult[LogExpr] = this.parseAll(expr, input)
}

object LogicalParser {

  def parse(input: List[String]): LogExpr = {
    if (input.isEmpty || input.forall(_.isEmpty))
      T
    else
      input.map(c => parse(c)).reduce((a, b) => AND(a, b))
  }

  def parse(input: String): LogExpr = {
    if (input.isEmpty)
      T
    else
      new LogicalParser().parse(input).getOrElse(Error(input))
  }

  sealed trait LogExpr
  case object T extends LogExpr
  case object F extends LogExpr
  case class EX(id: String) extends LogExpr
  case class LT(id: String, v: Double) extends LogExpr
  case class LE(id: String, v: Double) extends LogExpr
  case class GT(id: String, v: Double) extends LogExpr
  case class GE(id: String, v: Double) extends LogExpr
  case class EQ(id: String, v: Double) extends LogExpr
  case class NE(id: String, v: Double) extends LogExpr
  case class SEQ(id: String, v: String) extends LogExpr
  case class SNE(id: String, v: String) extends LogExpr
  case class AND(e1: LogExpr, e2: LogExpr) extends LogExpr
  case class OR(e1: LogExpr, e2: LogExpr) extends LogExpr
  case class Not(expr: LogExpr) extends LogExpr
  case class Error(msg: String) extends LogExpr

  def evalExists(logExpr: LogExpr)(vm: Map[String, List[String]]): Boolean ={
    logExpr match {
      case T => true
      case F => false
      case EX(id) => vm.contains(id) && vm(id).exists(x => x != "" && x != "0" )
      case LT(id, v) => vm(id).exists(_.toDouble < v)
      case LE(id, v) => vm(id).exists(_.toDouble <= v)
      case GT(id, v) => vm(id).exists(_.toDouble > v)
      case GE(id, v) => vm(id).exists(_.toDouble >= v)
      case EQ(id, v) => vm(id).exists(_.toDouble == v)
      case NE(id, v) => vm(id).exists(_.toDouble != v)
      case SEQ(id, v) => vm(id).contains(v)
      case SNE(id, v) => vm(id).exists(_ != v)
      case AND(_, F) => false
      case AND(e1, e2) => evalExists(e1)(vm) && evalExists(e2)(vm)
      case OR(_, T) => true
      case OR(e1, e2) => evalExists(e1)(vm) || evalExists(e2)(vm)
      case Not(e) => ! evalExists(e)(vm)
    }
  }

  def eval(logExpr: LogExpr)(vm: Map[String, String]): Boolean = {
    logExpr match {
      case T => true
      case F => false
      case EX(id) => vm.contains(id) && vm(id) != "" && vm(id) != "0"
      case LT(id, v) => vm(id).toDouble < v
      case LE(id, v) => vm(id).toDouble <= v
      case GT(id, v) => vm(id).toDouble > v
      case GE(id, v) => vm(id).toDouble >= v
      case EQ(id, v) => vm(id).toDouble == v
      case NE(id, v) => vm(id).toDouble != v
      case SEQ(id, v) => vm(id) == v
      case SNE(id, v) => vm(id) != v
      case AND(_, F) => false
      case AND(e1, e2) => eval(e1)(vm) && eval(e2)(vm)
      case OR(_, T) => true
      case OR(e1, e2) => eval(e1)(vm) || eval(e2)(vm)
      case Not(e) => ! eval(e)(vm)
    }
  }

  def view(logExpr: LogExpr): String = {
    logExpr match {
      case T => "true"
      case F => "false"
      case EX(id) => id
      case LT(id, v) => s"$id < $v"
      case LE(id, v) => s"$id <= $v"
      case GT(id, v) => s"$id > $v"
      case GE(id, v) => s"$id >= $v"
      case EQ(id, v) => s"$id == $v"
      case NE(id, v) => s"$id != $v"
      case SEQ(id, v) => s"$id == '$v'"
      case SNE(id, v) => s"$id != '$v'"
      case AND(e1, e2) => s"(${view(e1)}) and (${view(e2)})"
      case OR(e1, e2) => s"(${view(e1)}) or (${view(e2)})"
      case Not(e) => s"not (${view(e)})"
    }
  }

  def names(logExpr: LogExpr): Set[String] = {
    logExpr match {
      case T => Set[String]()
      case F => Set[String]()
      case EX(id) => Set(id)
      case LT(id, _) => Set(id)
      case LE(id, _) => Set(id)
      case GT(id, _) => Set(id)
      case GE(id, _) => Set(id)
      case EQ(id, _) => Set(id)
      case NE(id, _) => Set(id)
      case SEQ(id, _) => Set(id)
      case SNE(id, _) => Set(id)
      case AND(e1, e2) => names(e1) ++ names(e2)
      case OR(e1, e2) => names(e1) ++ names(e2)
      case Not(e) => names(e)
    }
  }
}
/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.seqspark.parser

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}
import scala.language.{higherKinds, existentials}

class ExprParser[num, repr[_]](variantAlg: VariantAlg[num, repr]) extends Parsers {
  override type Elem = ExprToken



  class ExprTokenReader(tokens: Seq[ExprToken]) extends Reader[ExprToken] {
    def first: ExprToken = tokens.head
    def atEnd: Boolean = tokens.isEmpty
    def pos: Position = NoPosition
    def rest: Reader[ExprToken] = new ExprTokenReader(tokens.tail)
  }

  private def identifier: Parser[Identifier] = {
    accept("identifier", {case id : Identifier => id})
  }


  def program: Parser[repr[_]] = {

  }

  def arithExpr: Parser[repr[num]] = positioned[repr[num]]{
    arithTerm ~ rep(Operator("+") ~ arithTerm | Operator("-") ~ arithTerm) ^^ {
      case t ~ ts => ts.foldLeft(t){
        case (a, Operator("+") ~ b) => variantAlg.numAlg.add(a, b)
        case (a, Operator("-") ~ b) => variantAlg.numAlg.sub(a, b)
      }
    }
  }

  def arithTerm: Parser[repr[num]] = positioned[repr[num]]{
    arithFactor ~ rep(Operator("*") ~ arithFactor | Operator("/") ~ arithFactor) ^^ {
      case t ~ ts => ts.foldLeft(t){
        case (a, Operator("*") ~ b) => variantAlg.numAlg.mul(a, b)
        case (a, Operator("/") ~ b) => variantAlg.numAlg.div(a, b)
      }
    }
  }

  def arithFactor: Parser[repr[num]]= positioned[repr[num]]{
    Delimiter("(") ~ arithExpr ~ Delimiter(")") ^^ {
      case Delimiter("(") ~ x ~ Delimiter(")") => x
    }
  }
}

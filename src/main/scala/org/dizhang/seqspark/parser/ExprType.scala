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
import ExprAST._

sealed trait ExprType

object ExprType {
  case object NumType extends ExprType
  case object StrType extends ExprType
  case object BoolType extends ExprType
  case object VarType extends ExprType
  case object FuncType extends ExprType

  def typeInfer(data: ExprAST): Either[TypeError, ExprType] = {
    data match {
      /** literal types */
      case Bool(_) => Right(BoolType)
      case Num(_) => Right(NumType)
      case Str(_) => Right(StrType)
      /** variable and function can be anything at this stage */
      case Variable(_) => Right(VarType)
      case Func(_, _) => Right(FuncType)
      /** logical OR */
      case Or(e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          /** errors in operands */
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          /** bool, var, and func operands */
          case (Right(BoolType)|Right(VarType)|Right(FuncType), Right(BoolType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          /** other types */
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in Or(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case And(e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          case (Right(BoolType)|Right(VarType)|Right(FuncType), Right(BoolType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in And(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case Not(e) =>
        val t = typeInfer(e)
        t match {
          case Left(_) => t
          case Right(BoolType)|Right(VarType)|Right(FuncType) => Right(BoolType)
          case Right(b) => Left(TypeError(s"type miss match in Not(t: ${b.toString})"))
        }
      case Comp(op, e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          case (Right(NumType)|Right(VarType)|Right(FuncType), Right(NumType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in ${op.toString}(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case Equal(op, e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          case (Right(NumType)|Right(VarType)|Right(FuncType), Right(NumType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          case (Right(StrType)|Right(VarType)|Right(FuncType), Right(StrType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          case (Right(BoolType)|Right(VarType)|Right(FuncType), Right(BoolType)|Right(VarType)|Right(FuncType)) =>
            Right(BoolType)
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in ${op.toString}(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case Sign(op, e) =>
        val t = typeInfer(e)
        t match {
          case Left(_) => t
          case Right(NumType)|Right(VarType)|Right(FuncType) => Right(NumType)
          case Right(b) => Left(TypeError(s"type miss match in ${op.toString}(t: ${b.toString})"))
        }
      case Add(op, e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          case (Right(NumType)|Right(VarType)|Right(FuncType), Right(NumType)|Right(VarType)|Right(FuncType)) =>
            Right(NumType)
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in ${op.toString}(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case Mul(op, e1, e2) =>
        val t1 = typeInfer(e1)
        val t2 = typeInfer(e2)
        (t1, t2) match {
          case (Left(_), _) => t1
          case (_, Left(_)) => t2
          case (Right(NumType)|Right(VarType)|Right(FuncType), Right(NumType)|Right(VarType)|Right(FuncType)) =>
            Right(NumType)
          case (Right(b1), Right(b2)) =>
            Left(TypeError(s"type miss match in ${op.toString}(t1: ${b1.toString}, t2: ${b2.toString})"))
        }
      case IfElse(cond, e1, e2) =>
        val t1 = typeInfer(cond)
        val t2 = typeInfer(e1)
        val t3 = typeInfer(e2)
        (t1, t2, t3) match {
          case (Left(_), _, _) => t1
          case (_, Left(_), _) => t2
          case (_, _, Left(_)) => t3
          case (Right(BoolType)|Right(VarType)|Right(FuncType), Right(x1), Right(x2)) =>
            if (x1 == x2) {
              t2
            } else if (x1 == VarType || x1 == FuncType) {
              t2
            } else if (x2 == VarType || x2 == FuncType) {
              t1
            } else {
              Left(TypeError(s"type miss match in ifelse(_, t1: ${x1.toString}, t2: ${x2.toString})"))
            }
          case (Right(x), _, _) =>
            Left(TypeError(s"type miss match in ifelse(cond: ${x.toString}, _, _)"))
        }
    }
  }
}

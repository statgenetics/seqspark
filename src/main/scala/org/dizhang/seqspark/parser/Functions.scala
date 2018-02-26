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

object Functions {

  sealed trait Func

  sealed trait StringFunction extends Func
  sealed trait NumberFunction extends Func
  sealed trait BooleanFunction extends Func
  sealed trait ReturnList extends Func

  case object Sum extends NumberFunction
  case object Min extends NumberFunction
  case object Max extends NumberFunction
  case object Mean extends NumberFunction
  case object Log extends NumberFunction
  case object Exp extends NumberFunction

  case object Exists extends BooleanFunction


}

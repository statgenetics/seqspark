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

package org.dizhang.seqspark.ds

import Pedigree._

/**
  * Created by zhangdi on 10/16/17.
  */
class Pedigree(val data: Array[Ped]) {
  //val graph: Map[String, Map[String, List[String]]]
}

object Pedigree {

  def apply(input: Seq[String]): Pedigree = {
    val data = input.map{l =>
      val s = l.split("\t")
      Ped(s(0), s(1), s(2), s(3), s(4).toInt, s(5).toInt)
    }.toArray
    new Pedigree(data)
  }

  case class Ped(fid: String, iid: String, pid: String, mid: String, sex: Int, aff: Int)
}
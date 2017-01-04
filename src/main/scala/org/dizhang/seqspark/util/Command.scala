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

import com.typesafe.config.Config

import scala.sys.process._
/**
  * This is for the task that can be run concurrently, include but not
  * limited to external commands like plink and king
  */


object Command {

  def annovar (input: String, output: String, workerDir: String)(implicit cnf: Config): Unit = {
    val progDir = cnf.getString("annotation.programDir")
    val build = cnf.getString(".genomeBuild")
    val annovardb = cnf.getString("annotation.databaseDir")
    val script = "%s/scripts/runAnnovar.sh" format (cnf.getString("seqaHome"))
    val cmd = s"${script} ${progDir} ${annovardb} ${build} ${input} ${output} ${workerDir}"
    println(cmd)
    cmd.!
  }
/**
  def popCheck (ini: Ini): String = {
    val project = ini.get("general", "project")
    val prefix = "results/%s/2sample" format (project)
    val all = "%s/all" format (prefix)
    val pruned = "%s/pruned" format (prefix)
    val plink = ini.get("sample", "plink")
    val king = ini.get("sample", "king")
    val hwe = ini.get("variant", "hwePvalue")
    val selectSNP = "%s --bfile %s --indep-pairwise 50 5 0.5 --allow-no-sex --out %s-list" format (plink, all, pruned)
    val makeBed = "%s --bfile %s --extract %s-list.prune.in --allow-no-sex --make-bed --out %s" format (plink, all, pruned, pruned)
    val runMds = "%s -b %s.bed --mds --ibs --prefix %s-mds" format (king, pruned, pruned)
    val runKinship = "%s -b %s.bed --kinship --related --degree 3 --prefix %s-kinship" format (king, pruned, pruned)
    var exitCode1: Int = 0
    var exitCode2: Int = 0
    var exitCode3: Int = 0
    var exitCode4: Int = 0
    try {
      exitCode1 = selectSNP.!
      exitCode2 = makeBed.!
      exitCode3 = runMds.!
      exitCode4 = runKinship.!
    } catch {
      case e: Exception => {println(e)}
    }
    "exit codes for mds and kinship are %d %d %d %d" format (exitCode1, exitCode2, exitCode3, exitCode4)
  }
  */
}

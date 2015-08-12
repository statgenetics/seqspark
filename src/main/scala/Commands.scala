import scala.sys.process._
import org.ini4j._
import org.apache.spark._
//import scala.concurrent._
//import ExecutionContext.Implicits.global

/**
  * This is for the task that can be run concurrently, include but not
  * limited to external commands like plink and king
  */


object Commands {

  def annovar (ini: Ini, input: String, output: String, workerDir: String): Unit = {
    val progDir = Option(ini.get("annotation", "programDir")).getOrElse("")
    val build = Option(ini.get("general", "build")).getOrElse("hg19")
    val annovardb = Option(ini.get("annotation", "annovardb")).getOrElse(List(progDir, "humandb").filter(_ != "").mkString("/"))
    val script = "runAnnovar.sh"
    val cmd = s"${script} ${progDir} ${annovardb} ${input} ${output} ${workerDir}"
    cmd.!
  }

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
}

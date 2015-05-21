import scala.sys.process._
import scala.concurrent._
import ExecutionContext.Implicits.global
import org.ini4j._
import org.apache.spark._

/**
  * This is for the task that can be run concurrently, include but not
  * limited to external commands like plink and king
  */


object Command {

  def runMds (ini: Ini, sc: SparkContext) {
    
    val project = ini.get("general", "project")
    val prefix = "results/%s/2sample"
    val bfile = "%s/all" format (prefix)
    val plink = ini.get("sample", "plink")
    val king = ini.get("sample", "king")
    val hwe = ini.get("variant", "hwePvalue")
    val selectSNP = "%s --bfile %s --hwe %s --indep-pairwise 50 5 0.5 --allow-no-sex --out %s/pruned" format (plink, bfile, hwe, prefix)

  }
}

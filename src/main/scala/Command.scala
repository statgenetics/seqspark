import scala.sys.process._
import scala.concurrent._
import ExecutionContext.Implicits.global
import org.ini4j._
import org.apache.spark._

/**
  * This is for the task that can be run concurrently, include but not
  * limited to external commands like plink and king
  */


object Mds {
  def readBedFromCeph(dir: String) {
    
  }

  def run (ini: Ini, sc: SparkContext) {
    
    /** 1. copy files from ceph back to local */
    val cephHome = ini.get("general", "cephHome")
    val project = ini.get("general", "project")
    val beds =
      if (cephHome != null)
        readBedFromCeph("%s/%s/9external/plink" format (cephHome, project))
      else
        sc.ObjectFile("%s/9external/plink" format (project)).collect
    val srcDir = "%s/"
    val copy = "cp "
    

  }
}

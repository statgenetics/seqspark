import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.ini4j._
import Utils._

object Pipeline {

  def run(modules: String)(implicit ini: Ini): Unit = {
    quickRun(modules)
  }

  /**
   * quick run. run through the specified modules
   * act as if the vcf is the only input
   */
  def quickRun(modules: String)(implicit ini: Ini) {
    val project = ini.get("general", "project")

    /** determine the input */
    val dirs = List("readvcf", "genotype", "sample", "variant", "annotation", "association")
    val rangeP = """(\d+)-(\d+)""".r
    val listP = """(\d+)(,\d+)+""".r
    val singleP = """(\d+)""".r
    val s = modules match {
      case rangeP(start, end) => (start.toInt to end.toInt).toList
      case listP(_*) => modules.split(",").map(_.toInt).toList
      case singleP(a) => List(a.toInt)
    }

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqA-%s" format project)
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[Bed], classOf[Var], classOf[Count[Pair]]))
    implicit val sc: SparkContext = new SparkContext(scConf)

    val raw: RawVCF = ReadVCF(ini.get("general", "vcf"))

    var current: VCF =
      if (s(0) == 1)
        GenotypeLevelQC(raw)
      else
        raw
    for (i <- s.slice(2, s.length - 1)) {
      val currentWorker = Worker.slaves(dirs(i))
      current.persist(StorageLevel.MEMORY_AND_DISK_SER)
      current = currentWorker(current)
    }

    Option(ini.get("general", "save")) match {
      case Some(x) => writeRDD(current.map(v => v.toString), "%s/%s.vcf" format (resultsDir, project))
      case None => {println("No need to save VCF.")}
    }
  }
}

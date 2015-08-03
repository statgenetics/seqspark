import org.apache.spark.rdd.RDD
import java.io._
import scala.io.Source

object Constant {
  object Pheno {
    val delim = "\t"
  }
  object Bt {
    val mis: Byte = -9
    val ref: Byte = 0
    val het: Byte = 1
    val mut: Byte = 2
    def conv(g: Byte): String = {
      g match {
        case Bt.het => Gt.het
        case Bt.mis => Gt.mis
        case Bt.mut => Gt.mut
        case Bt.ref => Gt.ref
        case _ => Gt.mis
      }
    }
    def flip(g: Byte): Byte = {
      g match {
        case Bt.het => het
        case Bt.mis => mis
        case Bt.ref => mut
        case Bt.mut => ref
        case _ => mis
      }
    }
  }
  object Gt {
    val mis = "./."
    val ref = "0/0"
    val het = "0/1"
    val mut = "1/1"
    def conv(g: String): Byte = {
      g match {
        case Gt.het => Bt.het
        case Gt.mis => Bt.mis
        case Gt.mut => Bt.mut
        case Gt.ref => Bt.ref
        case _ => Bt.mis
      }
    }
  }
  object Hg19 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 60000, 2699519), Region("X", 154931043, 155260559))
    val pseudoY = List(Region("Y", 10000, 2649519), Region("Y", 59034049, 59363565))
    val pseudo = pseudoX ::: pseudoY
    /** use a definition of MHC region from BGI 
      * url: 
      * http://bgiamericas.com/service-solutions/genomics/human-mhc-seq/
      */
    val mhc = Region(6.toByte, 29691115, 33054975)
  }
  object Hg38 {
    /** 0-based closed intervals, as with Region */
    val pseudoX = List(Region("X", 10000, 2781478), Region("X", 155701382, 156030894))
    val pseudoY = List(Region("Y", 10000, 2781478), Region("Y", 56887902, 57217414))
    val pseudo = pseudoX ::: pseudoY
  }
}

object Utils {
  type RawVar = Variant[String]
  type Var = Variant[Byte]
  type RawVCF = RDD[RawVar]
  type VCF = RDD[Var]
  type Pair = (Int, Int)
  import Constant._
/**
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }
  */
  def readColumn (file: String, col: String): Array[String] = {
    val delim = Pheno.delim
    val data = Source.fromFile(file).getLines.toList
    val header = data(0).split(delim).zipWithIndex.toMap
    val idx = header(col)
    val res =
      for (line <- data.slice(1, data.length))
      yield line.split(delim)(idx)
    res.toArray
  }

  def hasColumn (file: String, col: String): Boolean = {
    val delim = Pheno.delim
    val header = Source.fromFile(file).getLines.toList
    if (header(0).split(delim).contains(col))
      true
    else
      false
  }

  def writeArray(file: String, data: Array[String]) {
    val pw = new PrintWriter(new File(file))
    data foreach (d => pw.write(d + "\n"))
    pw.close
  }

  def writeAny(file: String, data: String): Unit = {
    val pw = new PrintWriter(new File(file))
    pw.write(data + "\n")
    pw.close()
  }

  def getFam (file: String): Array[String] = {
    val delim = Pheno.delim
    val data = Source.fromFile(file).getLines.toArray
    val res =
      for (line <- data.drop(1))
      yield line.split(delim).slice(0, 6).mkString(delim)
    res
  }

  def peek[A](dat: RDD[A]): Unit = {
    println("There are %s records like this:" format(dat.count()))
    println(dat.takeSample(false,1)(0))
  }
  /*def saveAsBed(vars: RawVCF, ini: Ini, dir: String) {
    val bed = vars.map(v => Bed(v))
//    val cephHome = ini.get("general", "cephHome")
//    if (cephHome != null)
//      saveInCeph(bed, "%s/%s" format(cephHome, dir))
//    else
//      bed.saveAsObjectFile(dir)
//    val fam = getFam(ini.get("general", "pheno"))
//    val famFile = "results/%s/2sample/all.fam" format (ini.get("general", "project"))
//    writeArray(famFile, fam)
//  }

  def saveInCeph(bed: RDD[Bed], dir: String) {

    def mkdir(dir: String) {
      val cephConf = "/etc/ceph/ceph.conf"
      val cephMnt = new CephMount()

      try {
        cephMnt.conf_read_file(cephConf)
        cephMnt.mount("/")
      } catch {
        case e: Exception => println("cannot mount ceph")
      }

      try {
        cephMnt.mkdirs(dir, BigInt("755", 8).toInt)
      } catch {
        case e: CephFileAlreadyExistsException => println("ceph dir %s alreadt exits, do nothing" format (dir))
      }

      cephMnt.unmount()
    }

    def savePart(iter: Iterator[Bed], idx: Int) {

      val bimFile = "%s/part-%s.bim" format (dir, idx)
      val bedFile = "%s/part-%s.bed" format (dir, idx)
      val bedMagical1 = Integer.parseInt("01101100", 2).toByte
      val bedMagical2 = Integer.parseInt("00011011", 2).toByte
      val bedMode = Integer.parseInt("00000001", 2).toByte
      val bedHead = Array[Byte](bedMagical1, bedMagical2, bedMode)
      val mode = BigInt("644", 8).toInt


      try {
        val cephConf = "/etc/ceph/ceph.conf"
        val cephMnt = new CephMount()
        //println("mark 1")
        cephMnt.conf_read_file(cephConf)
        //println("mark 2")
        cephMnt.mount("/")
        //println("mark 3")
        val bim0 = cephMnt.open(bimFile, CephMount.O_CREAT, mode)
        val bim1 = cephMnt.open(bimFile, CephMount.O_WRONLY, mode)
        val bed0 = cephMnt.open(bedFile, CephMount.O_CREAT, mode)
        val bed1 = cephMnt.open(bedFile, CephMount.O_WRONLY, mode)
        //println("mark 4")
        val data = iter reduce ((a, b) => Bed.add(a, b))
        //println("mark 5")
        cephMnt.write(bim1, data.bim, data.bim.length, 0)
        //println("mark 6")
        cephMnt.write(bed1, bedHead, 3, 0)
        //println("mark 7")
        cephMnt.write(bed1, data.bed, data.bed.length, 3)
        //println("mark 8")
        cephMnt.close(bim0)
        cephMnt.close(bim1)
        cephMnt.close(bed0)
        cephMnt.close(bed1)
        //println("mark 9")
        cephMnt.unmount()
        //println("mark 10")
      } catch {
        case e: Exception => {
          println("Could not save part %d for bed" format (idx))
          println(e)
        }
      }
    }

    mkdir(dir)

    bed.foreachPartition {p => val pid = TaskContext.get.partitionId; savePart(p, pid)}

  }*/

}

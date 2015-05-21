import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.SparkContext._
import org.ini4j._
import java.io._
import java.io.FileNotFoundException
import scala.io.Source
import sys.process._
import com.ceph.fs._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

object Constant {
  object Pheno {
    val delim = "\t"
  }
  object Gt {
    val mis = "./."
    val ref = "0/0"
    val het = "0/1"
    val mut = "1/1"
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
  type Var = Variant[String]
  type Data = RDD[Var]
  type Pair = (Double, Double)
  import Constant._
 
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

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

  def getFam (file: String): Array[String] = {
    val delim = Pheno.delim
    val data = Source.fromFile(file).getLines.toArray
    val res =
      for (line <- data.drop(1))
      yield line.split(delim).slice(0, 6).mkString(delim)
    res
  }

  def saveAsBed(vars: Data, ini: Ini, dir: String) {
    val bed = vars.map(v => Bed(v))
    val cephHome = ini.get("general", "cephHome")
    if (cephHome != null)
      saveInCeph(bed, "%s/%s" format(cephHome, dir))
    else
      bed.saveAsObjectFile(dir)
    val fam = getFam(ini.get("general", "pheno"))    
    val famFile = "results/%s/2sample/all.fam" format (ini.get("general", "project"))
    writeArray(famFile, fam)
   }

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
  }
}

object GenotypeLevel {
  import Constant._
  import Utils._

  /** compute call rate */
  def makeCallRate (g: String): Pair = {
    val gt = g.split(":")(0)
    if (gt == Gt.mis)
      (0.0, 1.0)
    else
      (1.0, 1.0)
  }

  /** compute maf of alt */
  def makeMaf (g: String): Pair = {
    val gt = g.split(":")(0)
    if (gt == Gt.mis)
      (0.0, 0.0)
    else if (gt == Gt.ref)
      (0.0, 2.0)
    else if (gt == Gt.het)
      (1.0, 2.0)
    else if (gt == Gt.mut)
      (2.0, 2.0)
    else
      (0.0, 0.0)
  }

  /** compute by GD, GQ */
  def statGdGq(vars: Data, ini: Ini) {
    type Cnt = Int2IntOpenHashMap
    type Bcnt = Map[Int, Cnt]
    val phenoFile = ini.get("general", "pheno")
    val batchCol = ini.get("pheno", "batch")
    val batchStr = readColumn(phenoFile, batchCol)
    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val gtFormat = ini.get("genotype", "format").split(":").zipWithIndex.toMap
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    def make(g: String): Cnt = {
      val s = g split (":")
      if (s(gtIdx) == Gt.mis)
        new Int2IntOpenHashMap(Array(0), Array(1))
      else {
        val key = s(gdIdx).toDouble.toInt * 100 + s(gqIdx).toDouble.toInt
        new Int2IntOpenHashMap(Array(key), Array(1))
      }
    }
    def count(vars: Data): Bcnt = {
      val all = vars map (v => Count[Cnt](v)(make).collapseByBatch(batchIdx))
      val res = all reduce ((a, b) => Count.addByBatch[Cnt](a, b))
      res
    }

    def writeBcnt(b: Bcnt) {
      val outDir =  "results/%s/1genotype" format (ini.get("general", "project"))
      val exitCode = "mkdir -p %s".format(outDir).!
      println(exitCode)
      val outFile = "%s/CountByGdGq.txt" format (outDir)
      val pw = new PrintWriter(new File(outFile))
      pw.write("batch\tgd\tgq\tcnt\n")
      for ((i, cnt) <- b) {
        val iter = cnt.keySet.iterator
        while (iter.hasNext) {
          val key = iter.next
          val gd: Int = key / 100
          val gq: Int = key - gd * 100
          val c = cnt.get(key)
          pw.write("%s\t%d\t%d\t%d\n" format (batchKeys(i), gd, gq, c))
        }
      }
      pw.close
    }
    writeBcnt(count(vars))
  }
}

object SampleLevel {
  import Constant._
  import Utils._

  def checkSex(vars: Data, ini: Ini) {
    /** Assume:
      *  1. genotype are cleaned before. 
      *  2. only SNV
      * */

    /** count heterozygosity rate */
    def makex (g: String): Pair = {
      if (g == Gt.mis)
        (0.0, 0.0)
      else if (g == Gt.het)
        (1.0, 1.0)
      else
        (0.0, 1.0)
    }
    
    /** count the call rate */
    def makey (g: String): Pair =
      if (g == Gt.mis) (0.0, 1.0) else (1.0, 1.0)
    
    /** count for all the SNVs on allosomes */
    def count (vars: Data): (Array[Pair], Array[Pair]) = {
      val pXY =
        if (ini.get("general", "build") == "hg19")
          Hg19.pseudo
        else
          Hg38.pseudo
      val allo = vars
        .filter (v => v.chr == "X" || v.chr == "Y")
        .filter (v => pXY forall (r => ! r.contains(v.chr, v.pos - 1)) )
      allo.cache
      val xVars = allo filter (v => v.chr == "X")
      val yVars = allo filter (v => v.chr == "Y")
      val emp: Array[Pair] =
        (0 until vars.take(1)(0).geno.length)
          .map (i => (0.0, 0.0))
          .toArray
      val xHetRate =
        if (xVars.isEmpty)
          emp
        else
          xVars
            .map (v => Count[Pair](v)(makex).geno)
            .reduce ((a, b) => Count.addGeno[Pair](a, b))
      val yCallRate =
        if (yVars.isEmpty)
          emp
        else
          yVars
            .map (v => Count[Pair](v)(makey).geno)
            .reduce ((a, b) => Count.addGeno[Pair](a, b))
      (xHetRate, yCallRate)
    }

    /** write the result into file */
    def write (res: (Array[Pair], Array[Pair])) {
      val pheno = ini.get("general", "pheno")
      val fids = readColumn(pheno, "FID")
      val iids = readColumn(pheno, "IID")
      val sex = readColumn(pheno, "Sex")
      val prefDir = "results/%s/2sample" format (ini.get("general", "project"))
      val exitCode = "mkdir -p %s".format(prefDir).!
      println(exitCode)
      val outFile = "%s/sexCheck.csv" format (prefDir)
      val pw = new PrintWriter(new File(outFile))
      pw.write("fid,iid,sex,xHet,xHom,yCall,yMis\n")
      val x = res._1 map (g => (g._1.toInt, g._2.toInt))
      val y = res._2 map (g => (g._1.toInt, g._2.toInt))
      for (i <- 0 until iids.length) {
        pw.write("%s,%s,%s,%d,%d,%d,%d\n" format (fids(i), iids(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
      }
      pw.close
    }
    write(count(vars))
  }

  def mds (vars: Data, ini: Ini) {
    /** 
      * Assume:
      *     1. genotype are cleaned before
      *     2. only SNVs
      */
    val pheno = ini.get("general", "pheno")
    val mdsMaf = ini.get("sample", "mdsMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m >= mdsMaf && m <= (1 - mdsMaf)) true else false

    val snp = VariantLevel.miniQC(vars, ini, pheno, mafFunc)
    //saveAsBed(snp, ini, "%s/9external/plink" format (ini.get("general", "project")))

    val bed = snp map (s => Bed(s))
    

    def write (bed: RDD[Bed]) {
      val pBed =
        bed mapPartitions(p => p reduce ((a, b) => List(Bed.add(a, b)).iterator))
      val prefix = "results/%s/2sample" format (ini.get("general", "project"))
      val bimFile = "%s/all.bim" format (prefix)
      val bedFile = "%s/all.bed" format (prefix)
      var bimStream = None: Option[FileOutputStream]
      var bedStream = None: Option[FileOutputStream]
      val iter = pBed.toLocalIterator
      try {
        bimStream = Some(new FileOutputStream(bimFile))
        bedStream = Some(new FileOutputStream(bedFile))
        while (iter.hasNext) {
          val cur = iter.next
          bimStream.get.write(cur.bim)
          bedStream.get.write(cur.bed)
        }
      } catch {
        case e: IOException => e.printStackTrace
      } finally {
        if (bimStream.isDefined) bimStream.get.close
        if (bedStream.isDefined) bedStream.get.close
      }
    }
    write(bed)
  }
}

object VariantLevel {
  import Utils._
  import Constant._
  
  def miniQC(vars: Data, ini: Ini, pheno: String, mafFunc: Double => Boolean): Data = {
    //val pheno = ini.get("general", "pheno")
    val batchCol = ini.get("pheno", "batch")
    val batchStr = readColumn(pheno, batchCol)
    val batchKeys = batchStr.zipWithIndex.toMap.keys.toArray
    val batchMap = batchKeys.zipWithIndex.toMap
    val batchIdx = batchStr.map(b => batchMap(b))
    val batch = batchIdx
    val misRate = ini.get("variant", "batchMissing").toDouble
    
    /** if call rate is high enough */
    def callRateP (v: Var): Boolean = {
      //println("!!! var: %s-%d geno.length %d" format(v.chr, v.pos, v.geno.length))
      val cntB = Count[Pair](v)(GenotypeLevel.makeCallRate).collapseByBatch(batch)
      val min = cntB.values reduce ((a, b) => if (a._1/a._2 < b._1/b._2) a else b)
      if (min._1/min._2 < (1 - misRate)) {
        //println("min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        false
      } else {
        //println("!!!min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        true
      }
    }

    /** if maf is high enough and not a batch-specific snv */
    def mafP (v: Var): Boolean = {
      val cnt = Count[Pair](v)(GenotypeLevel.makeMaf)
      val cntA = cnt.collapse
      val cntB = cnt.collapseByBatch(batch)
      val max = cntB.values reduce ((a, b) => if (a._1 > b._1) a else b)
      val maf = cntA._1/cntA._2
      val bSpec =
        if (! v.info.contains("DB") && max._1 > 1 && max._1 == cntA._1) {
          true
        } else {
          //println("bspec var %s-%d %f" format (v.chr, v.pos, max._1))
          false
        }
      //println("!!!!!! var: %s-%d maf: %f" format (v.chr, v.pos, maf))
      if (mafFunc(maf) && ! bSpec) true else false
    }
    //println("there are %d var before miniQC" format (vars.count))
    val snv = vars.filter(v => callRateP(v) && mafP(v))
    //println("there are %d snv passed miniQC" format (snv.count))
    snv
  }
}

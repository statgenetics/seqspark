import breeze.linalg.{SparseVector, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ini4j._
import java.io._
import scala.io.Source
import sys.process._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import Constants._
import Utils._

/** An abstract class that only contains a run method*/
trait Worker[A, B] {
  val name: WorkerName
  def apply(input: A)(implicit ini: Ini, sc: SparkContext): B
}

object Worker {
  val slaves = Map[String, Worker[VCF, VCF]](
    "sample" -> SampleLevelQC,
    "variant" -> VariantLevelQC,
    "annotation" -> Annotation)
}

object ReadVCF extends Worker[String, RawVCF] {
  implicit val name = new WorkerName("readvcf")

  def apply(input: String)(implicit ini: Ini, sc: SparkContext): RawVCF = {
    val raw = sc.textFile(input)
    makeVariants(raw)
  }

  /** filter variants based on
    * 1. filter column in vcf
    * 2. only bi-allelic SNPs if biAllelicSNV is true in Conf
    */
  def makeVariants(raw: RDD[String])(implicit ini: Ini): RawVCF = {
    val filterNot = ini.get("variant", "filterNot")
    val filter = ini.get("variant", "filter")
    val biAllelicSNV = ini.get("variant", "biAllelicSNV")
    val vars = raw filter (l => ! l.startsWith("#")) map (l => Variant(l))
    val s1 =
      if (filterNot != null)
        vars filter (v => ! v.filter.matches(filterNot))
      else
        vars
    val s2 =
      if (filter != null)
        s1 filter (v => v.filter.matches(filter))
      else
        s1
    val s3 =
      if (biAllelicSNV == "true")
        s2 filter (v => v.ref.matches("[ATCG]") && v.alt.matches("[ATCG]"))
      else
        s2
    /** save is very time-consuming and resource-demanding */
    if (ini.get("general", "save") == "true")
      try {
        s3.saveAsObjectFile("%s/1genotype" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    s3
  }
}

object GenotypeLevelQC extends Worker[RawVCF, VCF] {

  implicit val name = new WorkerName("genotype")

  def apply(input: RawVCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    statGdGq(input, ini)
    val gd = ini.get("genotype", "gd").split(",")
    val gq = ini.get("genotype", "gq").toDouble
    val gtFormat = ini.get("genotype", "format").split(":").zipWithIndex.toMap
    val gdLower = gd(0).toDouble
    val gdUpper = gd(1).toDouble
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    def make(g: String): String = {
      val s = g.split(":")
      if (s.length == 1)
        g
      else if (s(gtIdx) == Gt.mis)
        Gt.mis
      else if (s(gdIdx).toInt >= gdLower && s(gdIdx).toInt <= gdUpper && s(gqIdx).toDouble >= gq)
        s(gtIdx)
      else
        Gt.mis
    }

    val res = input.map(v => v.transElem(make(_)).compress(Gt.conv(_)))
    //res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (ini.get("genotype", "save") == "true")
      try {
        res.saveAsObjectFile("%s/2sample" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    res
  }

  /** compute call rate */
  def makeCallRate (g: Byte): Pair = {
    //val gt = g.split(":")(0)
    if (g == Bt.mis)
      (0, 1)
    else
      (1, 1)
  }

  /** compute maf of alt */
  def makeMaf (g: Byte): Pair = {
    //val gt = g.split(":")(0)
    g match {
      case Bt.mis => (0, 0)
      case Bt.ref => (0, 2)
      case Bt.het => (1, 2)
      case Bt.mut => (2, 2)
      case _ => (0, 0)
    }
  }

  /** compute by GD, GQ */
  def statGdGq(vars: RawVCF, ini: Ini) {
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

    def count(vars: RawVCF): Bcnt = {
      val all = vars map (v => Count[String, Cnt](v, make).collapseByBatch(batchIdx))
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

object SampleLevelQC extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("sample")

  def apply(input: VCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    checkSex(input, ini)

    mds(input, ini)

    val pheno = ini.get("general", "pheno")
    val newPheno = ini.get("general", "newPheno")
    val samples = readColumn(pheno, "IID")
    val removeCol = ini.get("pheno", "remove")
    val remove =
      if (hasColumn(pheno, removeCol))
        readColumn(pheno, removeCol)
      else
        Array.fill(samples.length)("0")

    /** save new pheno file after remove bad samples */
    val phenoArray = Source.fromFile(pheno).getLines.toArray
    val header = phenoArray(0)
    val newPhenoArray =
      for {
        i <- (0 until remove.length).toArray
        if (remove(i) != "1")
      } yield phenoArray(i + 1)
    writeArray(newPheno, header +: newPhenoArray)

    /** save cnt after remove bad samples */
    //import breeze.linalg.{}
    import Semi.SemiByte
    def make(gv: Vector[Byte]): Vector[Byte] = {
      val geno = gv.asInstanceOf[SparseVector[Byte]]
      geno.compact()
      val idx = geno.index
      val dat = geno.data
      val newIdx = idx.filter(p => remove(p) != 1)
      val newDat = dat.zip(idx).filter(p => remove(p._2) != 1).map(p => p._1)
      val res: Vector[Byte] = new SparseVector[Byte](newIdx, newDat, geno.size - (idx.size - newIdx.size))
      res
    }

    val res = input.map(v => v.transWhole(make(_)))
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (ini.get("sample", "save") == "true")
      try {
        res.saveAsObjectFile("%s/3variant" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step2: save failed"); System.exit(1)}
      }
    res
  }

  /** Check the concordance of the genetic sex and recorded sex */
  def checkSex(vars: VCF, ini: Ini) {
    /** Assume:
      *  1. genotype are cleaned before. 
      *  2. only SNV
      * */

    /** count heterozygosity rate */
    def makex (g: Byte): Pair = {
      if (g == Bt.mis)
        (0, 0)
      else if (g == Bt.het)
        (1, 1)
      else
        (0, 1)
    }
    
    /** count the call rate */
    def makey (g: Byte): Pair =
      if (g == Bt.mis) (0, 1) else (1, 1)
    
    /** count for all the SNVs on allosomes */
    def count (vars: VCF): (Array[Pair], Array[Pair]) = {
      val pXY =
        if (ini.get("general", "build") == "hg19")
          Hg19.pseudo
        else
          Hg38.pseudo
      val allo = vars
        .filter (v => v.chr == "X" || v.chr == "Y")
        .filter (v => pXY forall (r => ! r.contains(v.chr, v.pos.toInt - 1)) )
      allo.cache
      println("The number of variants on allosomes is %s" format (allo.count()))
      val xVars = allo filter (v => v.chr == "X")
      val yVars = allo filter (v => v.chr == "Y")
      val emp: Array[Pair] = Array.fill(vars.take(1)(0).geno.length)((0,0))
      val xHetRate =
        if (xVars.isEmpty)
          emp
        else {
          val tmp =
            ( xVars
              .map(v => Count[Byte, Pair](v, makex)) )

          //peek(tmp)
          val testVar = xVars.takeSample(false, 100)
          val testCnt = testVar.map(v => Count[Byte, Pair](v, makex))
          writeArray("Var", testVar.map(_.toString))
          writeArray("Cnt", testCnt.map(_.cnt.mkString("\t")))
          val sumCnt = testCnt.map(_.cnt).reduce((a, b) => Count.addGeno(a, b))
          writeAny("Sum", sumCnt.mkString("\t"))
          tmp.map(c => c.cnt).reduce((a, b) => Count.addGeno[Pair](a, b))
        }
      val yCallRate =
        if (yVars.isEmpty)
          emp
        else
          yVars
            .map (v => Count[Byte, Pair](v, makey).cnt)
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

  /** run the global ancestry analysis */
  def mds (vars: VCF, ini: Ini) {
    /** 
      * Assume:
      *     1. genotype are cleaned before
      *     2. only SNVs
      */
    val pheno = ini.get("general", "pheno")
    val mdsMaf = ini.get("sample", "mdsMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m >= mdsMaf && m <= (1 - mdsMaf)) true else false

    val snp = VariantLevelQC.miniQC(vars, ini, pheno, mafFunc)
    //saveAsBed(snp, ini, "%s/9external/plink" format (ini.get("general", "project")))
    println("\n\n\n\tThere are %s snps for mds" format(snp.count()))
    val bed = snp map (s => Bed(s))
    println("\n\n\n\tThere are %s beds for mds" format(bed.count()))
    def write (bed: RDD[Bed]) {
      val pBed =
        bed mapPartitions (p => List(p reduce ((a, b) => Bed.add(a, b))).iterator)
      /** this is a hack to force compute all partitions*/
      pBed.cache()
      pBed foreachPartition (p => None)
      val prefix = "results/%s/2sample" format (ini.get("general", "project"))
      val bimFile = "%s/all.bim" format (prefix)
      val bedFile = "%s/all.bed" format (prefix)
      var bimStream = None: Option[FileOutputStream]
      var bedStream = None: Option[FileOutputStream]
      val iter = pBed.toLocalIterator
      try {
        bimStream = Some(new FileOutputStream(bimFile))
        bedStream = Some(new FileOutputStream(bedFile))
        val bedMagical1 = Integer.parseInt("01101100", 2).toByte
        val bedMagical2 = Integer.parseInt("00011011", 2).toByte
        val bedMode = Integer.parseInt("00000001", 2).toByte
        val bedHead = Array[Byte](bedMagical1, bedMagical2, bedMode)
        bedStream.get.write(bedHead)
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
      pBed.unpersist()
    }
    write(bed)
    /**
    val f: Future[String] = future {
      Commands.popCheck(ini: Ini)
    }
    f onComplete {
      case Success(m) => println(m)
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
      */
  }
}

object VariantLevelQC extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("variant")

  def apply(input: VCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    val rareMaf = ini.get("variant", "rareMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m < rareMaf || m > (1 - rareMaf)) true else false

    val newPheno = ini.get("general", "newPheno")
    val afterQC = miniQC(input, ini, newPheno, mafFunc)
    val targetFile = ini.get("variant", "target")
    val select: Boolean = Option(targetFile) match {
      case Some(f) => true
      case None => false
    }
    val rare =
      if (select) {
        val iter = Source.fromFile(targetFile).getLines()
        val res =
          for {l <- iter
               s = l.split("\t")
          } yield "%s-%s".format(s(0), s(1)) -> (s(2) + s(3))
        val vMap = res.toMap
        afterQC.filter(v => vMap.contains("%s-%s".format(v.chr, v.pos)) && vMap("%s-%s".format(v.chr, v.pos)) == (v.ref + v.alt))
      } else
        afterQC

    rare.persist(StorageLevel.MEMORY_AND_DISK_SER)
    /** save is very time-consuming and resource-demanding */
    if (ini.get("variant", "save") == "true")
      try {
        rare.saveAsTextFile("%s/4association" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step3: save failed"); System.exit(1)}
      }
    rare
  }
  
  def miniQC(vars: VCF, ini: Ini, pheno: String, mafFunc: Double => Boolean): VCF = {
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
      //println("!!! var: %s-%d cnt.length %d" format(v.chr, v.pos, v.cnt.length))
      val cntB = Count[Byte, Pair](v, GenotypeLevelQC.makeCallRate).collapseByBatch(batch)
      val min = cntB.values reduce ((a, b) => if (a._1/a._2 < b._1/b._2) a else b)
      if (min._1/min._2.toDouble < (1 - misRate)) {
        //println("min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        false
      } else {
        //println("!!!min call rate for %s-%d is (%f %f)" format(v.chr, v.pos, min._1, min._2))
        true
      }
    }

    /** if maf is high enough and not a batch-specific snv */
    def mafP (v: Var): Boolean = {
      val cnt = Count[Byte, Pair](v, GenotypeLevelQC.makeMaf)
      val cntA = cnt.collapse
      val cntB = cnt.collapseByBatch(batch)
      val max = cntB.values reduce ((a, b) => if (a._1 > b._1) a else b)
      val maf = cntA._1/cntA._2.toDouble
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

object Annotation extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("annotation")

  def apply(input: VCF)(implicit ini: Ini, sc: SparkContext): VCF = {
    val rawSites = workerDir + "/sites.raw.vcf"
    val annotatedSites = workerDir + "/sites.annotated"
    writeRDD(input.map(_.meta().mkString("\t")), rawSites)
    Commands.annovar(ini, rawSites, annotatedSites, workerDir)
    val annot = sc.broadcast(readAnnot(annotatedSites))
    input.zipWithIndex().map{case (v, i: Long) => {
      val meta = v.meta().clone()
      meta(7) =
        if (meta(7) == ".")
          annot.value(i.toInt)
        else
          "%s;%s" format (meta(7), annot.value(i.toInt))
      Variant[Byte](meta, v.geno, v.flip)
    }}
  }

  def readAnnot(sites: String): Array[String] = {
    val varFile = sites + ".variant_function"
    val exonFile = sites + ".exonic_variant_function"
    val varArr: Array[(String, String)] =
      (for {
        l <- Source.fromFile(varFile).getLines()
        s = l.split("\t")
      } yield (s(0), s(1))).toArray

    val exonMap: Map[Int, String] =
      (for {
        l <- Source.fromFile(exonFile).getLines()
        s = l.split("\t")
      } yield s(0).substring(4).toInt -> s(1))
      .toMap
    varArr.zipWithIndex.map{case ((a, b), i: Int) =>
      if (a == "exonic")
        "ANNO=exonic:%s;GROUP=%s" format (exonMap(i + 1), b)
      else
        "ANNO=%s;GROUP=%s" format (a, b)}
  }
}


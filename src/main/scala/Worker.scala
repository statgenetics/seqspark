import breeze.collection.mutable.SparseArray
import org.apache.spark.rdd.RDD
import org.ini4j._
import java.io._
import sys.process._
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

object GenotypeLevel {
  import Constant._
  import Utils._

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

object SampleLevel {
  import Constant._
  import Utils._

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

    val snp = VariantLevel.miniQC(vars, ini, pheno, mafFunc)
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
      Command.popCheck(ini: Ini)
    }
    f onComplete {
      case Success(m) => println(m)
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
      */
  }
}

object VariantLevel {
  import Utils._
  import Constant._
  
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
      val cntB = Count[Byte, Pair](v, GenotypeLevel.makeCallRate).collapseByBatch(batch)
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
      val cnt = Count[Byte, Pair](v, GenotypeLevel.makeMaf)
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
/*
object Association {
  import Utils._

  def run (vars: VCF, ini: Ini): Unit = {


  }

}


object PostLevel {
  import Utils._

  def cut (vars: VCF, ini: Ini) {
    val newPheno = ini.get("general", "newPheno")
    val sexAbFile = ini.get("pheno", "sexAbFile")
    val ids = readColumn(newPheno, "IID")
    val sexSelf = readColumn(newPheno, "Sex")
    val sexAbnormal = readColumn(sexAbFile, "IID")
    val sex =
      for (i <- (0 until ids.length).toArray)
      yield if (sexAbnormal.contains(ids(i))) 0 else sexSelf(i)
    val race = readColumn(newPheno, "mds_race")
    def make (r: String, s: String, geno: Array[String]) = {
      for {
        i <- (0 until geno.length).toArray
        if (race(i) == r && sex(i) == s)
      } yield geno(i)
    }

    val aaFemale = vars.map(v => v.transWhole(make("African_American", "2", _)))
    val aaMale = vars.map(v => v.transWhole(make("African_American", "1", _)))
    val eaFemale = vars.map(v => v.transWhole(make("European_American", "2", _)))
    val eaMale = vars.map(v => v.transWhole(make("European_American", "1", _)))
    aaFemale.saveAsTextFile("%s/5aaFemale" format (ini.get("general", "project")))
    aaMale.saveAsTextFile("%s/6aaMale" format (ini.get("general", "project")))
    eaFemale.saveAsTextFile("%s/7eaFemale" format (ini.get("general", "project")))
    eaMale.saveAsTextFile("%s/8eaMale" format (ini.get("general", "project")))

  }
}
*/

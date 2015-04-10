import org.apache.spark.rdd.RDD
import java.io._
import org.apache.spark.SparkContext._
import org.ini4j._
import scala.io.Source

object Utils {
  type Data = RDD[Variant[String]]
  def readColumn (file: String, col: String, delim: String): List[String] = {
    val data = Source.fromFile(file).getLines.toList
    val header = data(0).split(delim).zipWithIndex.toMap
    val idx = header(col)
    val res =
      for (line <- data.slice(1, data.length))
      yield line.split(delim)(idx)
    res.toList
  }
 
  def saveAsBed (vars: Data, ini: Ini) {
    val map = Map("./." -> 1, "0/0" -> 0, "0/1" -> 2, "1/1" -> 3)
    val gd = ini.get("genotype", "gd").split(",")
    val gq = ini.get("genotype", "gq").toDouble
    val gtFormat = ini.get("genotype", "format").split(":").zipWithIndex.toMap
    val gdLower = gd(0).toDouble
    val gdUpper = gd(1).toDouble
    val gtIdx = gtFormat("GT")
    val gdIdx = gtFormat("GD")
    val gqIdx = gtFormat("GQ")
    val pheno = ini.get("general", "pheno")
    val p = Source.fromFile(pheno).getLines.toList
    val delim = ini.get("pheno", "delimiter")
    val header = p(0).split(delim).zipWithIndex.toMap
    val batchCol = ini.get("pheno","batch")
    val batchIdx = header(batchCol)
    val batch = for (line <- p.slice(1,p.length)) yield line.split(delim)(batchIdx).toInt
    val misRate = ini.get("variant", "batchMissing").toDouble
    val mdsMaf = ini.get("sample", "mdsMaf").toDouble
    val ctrl = readColumn(pheno, ini.get("pheno", "control"), delim)
    val sampleName = readColumn(pheno, "sample_name", delim)

    def miniQC (vars: Data, batch: List[Int]): Data = {
      type Cnt = (Double, Double)
      def makeCallRate (g: String): Cnt = {
        val s = g split (":")
        if (s(gtIdx) == "./." || s(gdIdx).toInt < gdLower || s(gdIdx).toInt > gdUpper || s(gqIdx).toDouble < gq)
          (0.0, 1.0)
        else
          (1.0, 1.0)
      }
      def makeMaf (g: String): Cnt = {
        val s = g split (":")
        if (s(gtIdx) == "./." || s(gdIdx).toInt < gdLower || s(gdIdx).toInt > gdUpper || s(gqIdx).toDouble < gq)
          (0.0, 0.0)
        else if (s(gtIdx) == "0/0")
          (0.0, 2.0)
        else if (s(gtIdx) == "0/1")
          (1.0, 2.0)
        else if (s(gtIdx) == "1/1")
          (2.0, 2.0)
        else
          (0.0, 0.0)
      }
      def callRateP (v: Variant[String]): Boolean = {
        val cntB = Count[Cnt](v)(makeCallRate).collapseByBatch(batch.toArray)
        val min = cntB.values reduce ((a, b) => if (a._1/a._2 < b._1/b._2) a else b)
        if (min._1/min._2 < (1 - misRate)) false else true
      }
      def mafP (v: Variant[String]): Boolean = {
        val cnt = Count[Cnt](v)(makeMaf)
        val cntA = cnt.collapse
        val cntB = cnt.collapseByBatch(batch.toArray)
        val max = cntB.values reduce ((a, b) => if (a._1 > b._1) a else b)
        val maf = cntA._1/cntA._2
        val bSpec = if (! v.info.contains("DB") && max._1 > 1 && max._1 == cntA._1) true else false
        if (maf >= mdsMaf && maf <= (1 - mdsMaf) && ! bSpec) true else false
      }
      vars filter (v => callRateP(v) && mafP(v))
    }

    def makeBed (vars: Data) {
      var bed = None: Option[FileOutputStream]
      val bim = new PrintWriter(new File("%s-plink.bim" format (ini.get("general", "project"))))
      val fam = new PrintWriter(new File("%s-plink.fam" format (ini.get("general", "project"))))

      def mkG (geno: Array[String], i: Int, j: Int): Byte = {
        val idx = 4 * i + j
        val s: Array[String] = if (idx < geno.length) geno(idx).split(":") else Array("./.")
        if (idx >= geno.length)
          0.toByte
        else if (s(gtIdx) == "./." || s(gdIdx).toInt < gdLower || s(gdIdx).toInt > gdUpper || s(gqIdx).toDouble < gq)
          1.toByte
        else if (s(gtIdx) == "0/0")
          0.toByte
        else if (s(gtIdx) == "0/1" || s(gtIdx) == "1/0")
          2.toByte
        else if (s(gtIdx) == "1/1")
          3.toByte
        else
          1.toByte
      }

      def convert (v: Variant[String]): List[Byte] = {
        val res =
          for {
            i <- 0 to v.geno.length/4
            four = 0 to 3 map (j => mkG(v.geno, i, j))}
          yield
            four.zipWithIndex.map(a => a._1 << 2 * a._2).sum.toByte
        res.toList
      }

      try {
        bed = Some(new FileOutputStream("%s-plink.bed" format (ini.get("general", "project"))))
        val magical1 = Integer.parseInt("01101100", 2).toByte
        val magical2 = Integer.parseInt("00011011", 2).toByte
        val mode = Integer.parseInt("00000001", 2).toByte
        bed.get.write(magical1)
        bed.get.write(magical2)
        bed.get.write(mode)

        /** fam file */
        for (i <- 0 until sampleName.length) {
          val status = ctrl(i) match {
            case "1" => 1
            case "0" => 2
            case "NA" => 0
            case _ => 0
          }
          fam.write("%d\t%s\t0\t0\t0\t%d\n" format (i+1, sampleName(i), status))
        }

        /** bed and bim file */
        for (p <- parts) {
          val idx = p.index
          val partRdd = vars.mapPartitionsWithIndex((a, b) => if (a == idx) b else Iterator(), true)
          val data = partRdd.collect
          for (v <- data) {
            val snp = "%s-%d" format (v.chr, v.pos)
            bim.write("%s\t%s\t0\t%d\t%s\t%s\n" format (v.chr, snp, v.pos, v.ref, v.alt))
            for (c <- convert(v)) {
              bed.get.write(c)
            }
          }
        }
      } catch {
        case e: IOException => e.printStackTrace
      } finally {
        println("plink bed file generated!")
        if (bed.isDefined) bed.get.close
        bim.close
        fam.close
      }
    }
    makeBed(miniQC(vars, batch))

  }

  def inter(vars: RDD[Variant[String]], batch: Array[Int]) {
    //type Cnt = Map[Int, Int]
    type Cnt = (Double, Double)
    def make(g: String): Cnt = {
      val s = g split (":")
      //val gd = s(3).toInt
      //val gq = s(4).toDouble
      if (s(0) == "./.") (0.0, 1.0) else (1.0, 1.0)
    }
    def interP(v: Variant[String]): Boolean = {
      val cntB = Count[Cnt](v)(make).collapseByBatch(batch)
      val min = cntB.values reduce ((a, b) => if (a._1/a._2 < b._1/b._2) a else b)
      if (min._1/min._2 < 0.9) false else true
    }
    def interV (vars: RDD[Variant[String]]): RDD[Variant[String]] =
      vars filter (v => interP(v))

    def writeInter (vars: RDD[Variant[String]]) {
      val file = "Bspec.txt"
      val pw = new PrintWriter(new File(file))
      val res = vars filter (v => ! interP(v)) map (x => (x.chr, x.pos, x.filter))
      pw.write("chr\tpos\tfilter\n")
      res.collect foreach { case (c, p, f) => pw.write("%s\t%d\t%s\n" format (c, p, f)) }
      pw.close
    }
    writeInter(vars)
    //interV(vars)
  }

  def countByGD(vars: RDD[Variant[String]], file: String, batch: Array[Int]) {
    type Cnt = Map[(Int, Int), Int]
    type Bcnt = Map[Int, Cnt]
    def make(g: String): Cnt = {
      val s = g split (":")
      if (s(0) == "./.") Map((0, -1) -> 0) else Map((s(3).toInt, s(4).toInt) -> 1)
    }
    def cnt(vars: RDD[Variant[String]]): Array[(String, Bcnt)] = {
      val all = vars map (
        v => (v.filter, Count[Cnt](v)(make).collapseByBatch(batch)))
      val res = all reduceByKey ((a, b) => Count.addByBatch[Cnt](a, b))
      res.collect
    }
    def writeCnt(cnt: Array[(String, Bcnt)], file: String) {
      val pw = new PrintWriter(new File(file))
      pw.write("qc\tbatch\tgd\tgq\tcnt\n")
      for ((qc, map1) <- cnt)
        for ((batch, map2) <- map1)
          for (((gd, gq), c) <- map2)
            pw.write("%s\t%d\t%d\t%d\t%d\n" format (qc, batch, gd, gq, c))
      pw.close
    }
    writeCnt(cnt(vars), file)
  }

  def countByFilter(vars: RDD[Variant[String]], file: String) {
    val pw = new PrintWriter(new File(file))
    val cnts: RDD[(String, Int)] =
      vars map (v => (v.filter, 1)) reduceByKey ((a: Int, b: Int) => a + b)
    cnts.collect foreach {case (f: String, c: Int) => pw.write(f + ": " + c + "\n")}
    pw.close
  }

  def computeMis(vars: RDD[Variant[String]], file: String, qc: Boolean, group: Map[(String, Int), String] = null) = {
    val grp = vars.map(v => (v.chr, v.pos) -> v.filter).collect.toMap
    def pass(g: String): Boolean = {
      val s = g.split(":")
      if (s(0) == "./." || (qc && s(1) != "PASS")) false else true
    }
    def make(g: String): Int = {
      if (pass(g)) 1 else 0
    }
    def cnt(vars: RDD[Variant[String]]): Map[(String, Int), Int] = {
      vars.map(v => (v.chr, v.pos) -> v.geno.map(g => make(g)).reduce((a,b) => a + b)).collect.toMap
    }
    def writeMis(c: Map[(String, Int), Int], file: String) {
      val pw = new PrintWriter(new File(file))
      pw.write("chr\tpos\tcnt\tqc\tfreq\n")
      c.foreach {case ((chr, pos), cnt) => pw.write("%s\t%d\t%d\t%s\t%s\n" format(chr, pos, cnt,grp((chr, pos)), group((chr, pos)) ))}
      pw.close
    }
    writeMis(cnt(vars), file)
  }

  def computeMaf(vars: RDD[Variant[String]], qc: Boolean) = {
    def pass(g: String): Boolean = {
      val s = g.split(":")
      if (s(0) == "./.") false else true}
    def pass1(g: String) = {
      val s = g.split(":")
      if (s(0) == "./." || (qc && s(1) != "PASS")) false else true
    }
    def make(g: String): Gq = {
      val s = g.split(":")
      if (pass(g)){
        val a = s(0).split("/").map(_.toInt)
        new Gq((a.sum, 2))
      }else
        new Gq((0, 0))
    }
    def maf(vars: RDD[Variant[String]]): Map[(String, Int), String] = {
      val m = vars.map(v => {val maf = v.geno.map(g => make(g)).reduce((a, b) => a + b); val cat = if (maf.cnt == 0) "Nocall" else if (maf.total == 1 || maf.cnt - maf.total == 1) "Singleton" else if (maf.mean < 0.01 || maf.mean > 0.99) "Rare" else if (maf.mean < 0.05 || maf.mean > 0.95) "Infrequent" else "Common"; (v.chr, v.pos, cat)}).collect
      m.map(v => (v._1, v._2) -> v._3).toMap
    }
    maf(vars)
  }

  def computeGQ(vars: RDD[Variant[String]], file: String, ids: Array[String], group: Map[(String, Int), String] = null) {
    val grp = Option(group).getOrElse(vars.map(v => (v.chr, v.pos) -> v.filter).collect.toMap)
    def pass(g: String): Boolean = {
      val s = g.split(":")
      if (s(0) == "./.") false else true}
    def make(g: String): Gqmap = {
      val s = g.split(":")
      if (pass(g))
        new Gqmap(scala.collection.mutable.Map[String, Gq](s(0) -> new Gq((s(4).toDouble, 1))))
      else
        new Gqmap(scala.collection.mutable.Map[String, Gq]("./." -> new Gq((0, 1))))}
    def add (a: Gtinfo[Gqmap], b: Gtinfo[Gqmap]) = {
      for (i <- Range(0, a.length))
        a.elems(i) += b.elems(i)
      a}

    def gq(vars: RDD[Variant[String]]): Array[(String, Gtinfo[Gqmap])] = {
      vars.map(v => (grp((v.chr, v.pos)), new Gtinfo[Gqmap]{val elems =v.geno.map(g => make(g))})).reduceByKey((a: Gtinfo[Gqmap], b: Gtinfo[Gqmap]) => add(a, b)).collect
    }
    def writeGQ(gq: Array[(String, Gtinfo[Gqmap])], file: String) {
      val pw = new PrintWriter(new File(file))
      val delim = "\t"
      //val header = gq.map(x => x._2.elems(0).headers.map(h => x._1 + h).mkString(delim)).mkString(delim)
      val header = "group\tsample\tgt\ttotal\tcnt\n"
      pw.write(header)
      for (j <- Range(0, gq.length)) {
        for (i <- Range(0,gq(0)._2.length)) {
          val g =  gq(j)._2.elems(i).elem
          for (k <- g.keys
            if (k != "./."))
            pw.write("%s\t%s\t%s\t%s\t%s\n".format(gq(j)._1, ids(i),k, g(k).elem._1, g(k).elem._2))
        }
      }
      pw.close
    }
    writeGQ(gq(vars), file)
  }

  def callRate (vars: RDD[Variant[String]], file: String, id: Array[String]) {
    type Cnt = (Double, Double)
    
    def make (g: String): Cnt = {
      val s = g.split(":")
      //if (s(0) == "./." || s(1).toDouble < 8.0 || s(2).toDouble < 20.0) (0.0, 1.0) else (1.0, 1.0)
      if (s(0) == "./.") (0.0, 1.0) else (1.0, 1.0)
    }

    def cnt (vars: RDD[Variant[String]]): Count[Cnt] = {
      vars map (v => Count[Cnt](v)(make)) reduce ((a: Count[Cnt], b: Count[Cnt]) => a add b)
    }

    def writeCnt (c: Count[Cnt], file: String, id: Array[String]) {
      val pw = new PrintWriter(new File(file))
      pw.write("id\tcall\ttotal\trate\n")
      for ((i, (cnt, total)) <- id zip c.geno )
        pw.write("%s\t%s\t%s\t%.4f\n" format (i, cnt, total, cnt/total))
      pw.close
    }
    writeCnt(cnt(vars), file, id)
  }

  def checkSex(vars: RDD[Variant[String]], pheno: String, ini: Ini) {
    
    def getPhenoSex(pheno: String, ini: Ini): (List[String], List[String]) = {
      //try {
      val p = Source.fromFile(pheno).getLines.toList
      //} catch {
      //  case ex: FileNotFoundException => System.exit(1)
      //}
      val sexCol = ini.get("pheno", "sex")
      val delim = ini.get("pheno", "delimiter")
      val header = p(0).split(delim)
      val sexMap = header.zipWithIndex.toMap
      val sexIdx = sexMap(sexCol)
      val ids = for (i <- 1 until p.length) yield p(i).split(delim)(0)
      val sex = for (i <- 1 until p.length) yield p(i).split(delim)(sexIdx)
      (ids.toList, sex.toList)
    }
    
    type Cnt = (Double, Double)
    
    def makex (g: String): Cnt = {
      val s = g.split(":")
      if (s(0) == "./." || s(3).toDouble < 8 || s(4).toDouble < 20) (0.0, 0.0) else if (s(0) == "0/1") (1.0, 1.0) else (0.0, 1.0)
    }

    def makey (g: String): Cnt = {
      val s = g.split(":")
      if (s(0) == "./." || s(3).toDouble < 8 || s(4).toDouble < 20) (0.0, 1.0) else (1.0, 1.0)
    }

    def inferSex (vars: RDD[Variant[String]]): (Count[Cnt], Count[Cnt]) = {
      val pX = List((60001, 2699520), (154931044, 155260560))
      val pY = List((10001, 2649520), (59034050, 59363566))
      val tmp = vars filter (v => v.chr == "X" || v.chr == "Y")
      val allo = tmp filter (v => v.ref.matches("^[ATCG]$") && v.alt.matches("^[ATCG]$"))
      allo.cache()
      val x = allo filter (v => v.chr == "X" && v.pos > pX(0)._2 && v.pos < pX(1)._1) map (v => Count[Cnt](v)(makex)) reduce ((a: Count[Cnt], b: Count[Cnt]) => a add b)
      val y = allo filter (v => v.chr == "Y" && v.pos > pY(0)._2 && v.pos < pY(1)._1) map (v => Count[Cnt](v)(makey)) reduce ((a: Count[Cnt], b: Count[Cnt]) => a add b)
      (x, y)
    }

    def write (idsex: (List[String], List[String]) , res: (Count[Cnt], Count[Cnt]), file: String) {
      val pw = new PrintWriter(new File(file))
      pw.write("id\tsex\tXhet\tXhom\tYcall\tYmis\n")
      val ids = idsex._1
      val sex = idsex._2
      val x = res._1.geno map (g => (g._1.toInt, g._2.toInt))
      val y = res._2.geno map (g => (g._1.toInt, g._2.toInt))
      for (i <- 0 until ids.length) {
        pw.write("%s,%s,%d,%d,%d,%d\n" format (ids(i), sex(i), x(i)._1, x(i)._2, y(i)._1, y(i)._2))
      }
      pw.close
    }

    write(getPhenoSex(pheno, ini), inferSex(vars), "sexCheck")

    def test (vars: RDD[Variant[String]]) {
      val pw = new PrintWriter(new File("sex.vcf"))
      val sex = vars filter (v => v.chr == "X" || v.chr == "Y") map (v => v.geno.slice(0,99))
      sex.collect foreach {
        g => pw.write(g.mkString("\t") + "\n")
      }
      pw.close
    }
    //test(vars)

  }

}

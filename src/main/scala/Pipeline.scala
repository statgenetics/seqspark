import breeze.linalg.{Vector, SparseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import org.ini4j._
import Constant._
import Utils._

object Pipeline {
  /** type alias to save typing */
  //type Data = RDD[Variant[String]]

  /**
   * quick run. run through the specified steps
   * act as if the vcf is the only input
   */
  def quickRun(ini:Ini, steps: String) {
    val project = ini.get("general", "project")

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqA-%s" format (project))
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[Bed], classOf[Var], classOf[Count[Pair]]))
    val sc = new SparkContext(scConf)

    /** determine the input*/
    val dirs = List("1genotype", "2sample", "3variant", "4association")
    val s = steps.split("-").map(_.toInt)

    //val file = "%s/%s/all.vcf" format (project, dirs(s(0) - 1))
    //println(file)
    //val vars = sc.textFile(file) filter (_ != "") map (l => Variant(l))
    //postProcess(vars, ini)
    var rawVCF: RawVCF = null
    var preVar: VCF = null
    if (s(0) == 0) {
      val raw = sc.textFile(ini.get("general", "vcf"))
      rawVCF = makeVariants(raw, ini)
      rawVCF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    } else {
      val input = "%s/%s" format(ini.get("general", "project"), dirs(s(0) - 1))
      preVar = sc.objectFile(input)
      preVar.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    val vars1 =
      if (1 >= s(0) && 1 <= s.last && rawVCF != null)
        genotype(rawVCF, ini)
      else
        preVar
    val vars2 =
      if (2 >= s(0) && 2 <= s.last)
        sample(vars1, ini)
      else
        vars1
    //println("var number after genotype qc: %s" format (vars1.count))
    //val test = vars2.takeSample(false, 1)(0)
    //writeAny("test", test.toString)

    vars2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val vars3 =
      if (3 >= s(0) && 3 <= s.last)
        variant(vars2, ini)
      else
        vars2
    select(vars3, ini).map(v => Variant.makeVCF(v)).saveAsTextFile("%s/%s" format(ini.get("general", "project"), dirs(s.last)))

    /*
            if (4 >= s(0) && 4<= s.last) {
              association(vars3)
            }
        */
    /* cut the vcf by sample group */
    //if (4 >= s(0) && 4 <= s.last)
    //  postProcess(vars3, ini)

  }


  /** filter variants based on 
    * 1. filter column in vcf 
    * 2. only bi-allelic SNPs if biAllelicSNV is true in Conf
    */
  def makeVariants(raw: RDD[String], ini: Ini): RawVCF = {
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

  /**
    * Genotyoe level QC
    * 1. compute various stats for genotypes
    * 2. use SNVs on allosomes to check Sex
    */
  def genotype (vars: RawVCF, ini: Ini): VCF = {
    import GenotypeLevel._

    statGdGq(vars, ini)
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
      if (s(gtIdx) == Gt.mis)
        Gt.mis
      else if (s(gdIdx).toInt >= gdLower && s(gdIdx).toInt <= gdUpper && s(gqIdx).toDouble >= gq)
        s(gtIdx)
      else
        Gt.mis
    }

    val res = vars.map(v => v.transElem(make(_)).compress(Gt.conv(_)))
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** save is very time-consuming and resource-demanding */
    if (ini.get("genotype", "save") == "true")
      try {
        res.saveAsObjectFile("%s/2sample" format (ini.get("general", "project")))
      } catch {
        case e: Exception => {println("step1: save failed"); System.exit(1)}
      }
    res
  }
  
  def sample (vars: VCF, ini: Ini): VCF = {
    import SampleLevel._

    checkSex(vars, ini)

    mds(vars, ini)

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
    val res = vars.map(v => v.transWhole(make(_)))
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

  def variant (vars: VCF, ini: Ini): VCF = {
    import VariantLevel._

    val rareMaf = ini.get("variant", "rareMaf").toDouble
    def mafFunc (m: Double): Boolean =
      if (m < rareMaf || m > (1 - rareMaf)) true else false

    val newPheno = ini.get("general", "newPheno")
    println("we have %d variants in step3" format (vars.count))
    val rare = miniQC(vars, ini, newPheno, mafFunc)
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

  def select (vars: VCF, ini: Ini): VCF = {
    val targetFile = ini.get("variant", "table")
    val iter = Source.fromFile(targetFile).getLines()
    val res =
      for {l <- iter
           s = l.split("\t")
      } yield "%s-%s".format(s(0), s(1)) -> (s(2) + s(3))
    val vMap = res.toMap
    vars.filter(v => vMap.contains("%s-%s".format(v.chr, v.pos)) && vMap("%s-%s".format(v.chr, v.pos)) == (v.ref + v.alt))
  }

/*
  def association (vars: VCF, ini: Ini): Unit = {
    import Association._

    run (vars, ini)

  }

  def postProcess (vars: VCF, ini: Ini) {
    import PostLevel.cut
    vars.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("we have %d variants in post processing step" format (vars.count))
    cut(vars, ini)

  }
  */
}

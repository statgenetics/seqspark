import org.apache.spark.rdd.RDD
import java.io._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
    */
  def quickRun(ini:Ini, steps: String) {
    val project = ini.get("general", "project")

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqA-%s" format (project))
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[Tped], classOf[Bed], classOf[Var], classOf[Count[Pair]]))
    val sc = new SparkContext(scConf)

    /** determine the input*/
    val dirs = List("1genotype", "2sample", "3variant", "4association")
    val s = steps.split("-").map(_.toInt)
    val raw = sc.textFile(ini.get("general", "vcf"))
    val file = "%s/%s/all.vcf" format (project, dirs(s(0) - 1))
    println(file)
    val vars = sc.textFile(file) filter (_ != "") map (l => Variant(l))
    postProcess(vars, ini)
    /*
    val vars0: Data =
      if (s(0) == 0)
        makeVariants(sc.textFile(ini.get("general", "vcf")), ini)
      else
        sc.textFile("%s/%s" format(project, dirs(s(0) - 1))) map (l => Variant(l))
    vars0.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val vars1 =
      if (1 >= s(0) && 1 <= s.last)
        genotype(vars0, ini)
      else
        vars0
    vars0.unpersist()
    val vars2 =
      if (2 >= s(0) && 2 <= s.last)
        sample(vars1, ini)
      else
        vars1
    vars1.unpersist()
    val vars3 =
      if (3 >= s(0) && 3 <= s.last)
        variant(vars2, ini)
      else
        vars2
    vars2.unpersist()
    /* cut the vcf by sample group */
    if (4 >= s(0) && 4 <= s.last)
      postProcess(vars3, ini)
     */
  }


  /** filter variants based on 
    * 1. filter column in vcf 
    * 2. only bi-allelic SNPs if biAllelicSNV is true in Conf
    */
  def makeVariants(raw: RDD[String], ini: Ini): Data = {
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
  def genotype (vars: Data, ini: Ini): Data = {
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



    val res = vars.map(v => Variant.transElem(v, make(_)))
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
  
  def sample (vars: Data, ini: Ini): Data = {
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

    /** save geno after remove bad samples */
    def make(geno: Array[String]): Array[String] = {
      for {
        i <- (0 until geno.length).toArray
        if (remove(i) != "1")
      } yield geno(i)
    }
    val res = vars.map(v => Variant.transWhole(v, make(_)))
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

  def variant (vars: Data, ini: Ini): Data = {
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

  def postProcess (vars: Data, ini: Ini) {
    import PostLevel.cut
    vars.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("we have %d variants in post processing step" format (vars.count))
    cut(vars, ini)

  }
}

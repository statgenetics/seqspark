package org.dizhang.seqspark

import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.ds.{Bed, Counter, Genotype, Phenotype}
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import com.typesafe.config.ConfigFactory
import java.io.File

import scala.collection.JavaConverters._
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.assoc.AssocMaster
import org.dizhang.seqspark.util.{SingleStudyContext, UserConfig}
import org.dizhang.seqspark.worker.{Import, QualityControl}
import org.slf4j.LoggerFactory
import util.General._

/**
 * Main function
 */

object SingleStudy {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    /** check args */
    if (badArgs(args)) {
      logger.error(s"bad argument: ${args.mkString(" ")}")
      System.exit(1)
    }

    /** quick run */
    val userConfFile = new File(args(0))
    require(userConfFile.exists())
    try {
      val userConf = ConfigFactory
        .parseFile(userConfFile)
        .withFallback(ConfigFactory.load().getConfig("seqspark"))
        .resolve()

      val show = userConf.root().render()

      logger.debug("Conf detail:\n" + show)

      val rootConf = RootConfig(userConf)
      if (checkConf(rootConf))
        run(rootConf)
    } catch {
      case e: Exception => {
        logger.error("Something went wrong, exit")
        e.printStackTrace()
      }
    }
  }


  def checkConf(conf: RootConfig): Boolean = {
    if (! annot.CheckDatabase.qcTermsInDB(conf)) {
      logger.error("conf error in qualityControl")
      false
    } else if (! annot.CheckDatabase.annTermsInDB(conf)) {
      logger.error("conf error in annotation")
      false
    } else if (conf.pipeline.last == "association" && ! annot.CheckDatabase.assTermsInDB(conf)) {
      logger.error("conf error in association method variants")
      false
    } else {
      logger.info("Conf file fine")
      true
    }
  }

  def run(cnf: RootConfig) {
    val project = cnf.project

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqA-%s" format project)
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[Bed], classOf[Var], classOf[Counter[(Double, Double)]]))
    val sc: SparkContext = new SparkContext(scConf)

    val pipeline = cnf.pipeline

    val phenotype = Phenotype(cnf.input.phenotype.path, sc)

    implicit val ssc = SingleStudyContext(cnf, sc, phenotype)

    if (cnf.input.genotype.format == UserConfig.ImportGenotypeType.vcf) {
      val clean = try {
        logger.info("read from cache")
        sc.objectFile(cnf.project).asInstanceOf[worker.Data[Byte]]
      } catch {
        case e: Exception =>
          logger.info("no cache, compute from start")
          val res = QualityControl.cleanVCF(Import.fromVCF(ssc))
          res.persist(StorageLevel.MEMORY_AND_DISK)
          res.saveAsObjectFile(cnf.project)
          res
      }
      runAssoc(clean)
    } else if (cnf.input.genotype.format == UserConfig.ImportGenotypeType.imputed) {
      val clean = try {
        logger.info("read from cache")
        sc.objectFile(cnf.project).asInstanceOf[worker.Data[(Double, Double, Double)]]
      } catch {
        case e: Exception =>
          logger.info("no cache, compute from start")
          val res = QualityControl.cleanImputed(Import.fromImpute2(ssc))
          res.persist(StorageLevel.MEMORY_AND_DISK)
          res.saveAsObjectFile(cnf.project)
          res
      }
      runAssoc(clean)
    } else {
      logger.error(s"unrecognized genotype format ${cnf.input.genotype.format.toString}")
    }

    /**
    val binder = org.slf4j.impl.StaticLoggerBinder.getSingleton
    logger.debug("hellor world")
    logger.debug(binder.getLoggerFactoryClassStr)
    logger.info(s"pipeline: ${(("import" :: pipeline) ::: List("export")).mkString(" ")}")
    */
    //val current = Import({})

    //val last = WorkerObsolete.recurSlaves(current, pipeline)

    //Export(last)
    //PropertyConfigurator.configure("log4j.properties")
  }

  def runAssoc[A: Genotype](input: worker.Data[A])
                           (implicit ssc: SingleStudyContext): Unit = {
    if (ssc.userConfig.pipeline.length > 1) {
      val assocConf = ssc.userConfig.association
      val methods = assocConf.methodList
      val annotated = if (methods.exists(m =>
        assocConf.method(m).misc.getStringList("groupBy").asScala.contains("gene"))) {
        annot.linkGeneDB(input)(ssc.userConfig, ssc.sparkContext)
      } else {
        input
      }
      annotated.persist(StorageLevel.MEMORY_AND_DISK)
      val assoc = new AssocMaster(annotated)(ssc)
      assoc.run()
    }
  }



}

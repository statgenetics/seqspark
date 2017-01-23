/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.dizhang.seqspark.assoc.AssocMaster
import org.dizhang.seqspark.ds.{Bed, Counter, Genotype, Phenotype}
import org.dizhang.seqspark.util.General._
import org.dizhang.seqspark.util.InputOutput._
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.util.{SingleStudyContext, UserConfig}
import org.dizhang.seqspark.worker.{Import, QualityControl}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

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

    try {

      val rootConf = readConf(args(0))

      if (checkConf(rootConf)) {
        run(rootConf)
      } else {
        logger.error("Configuration error, exit")
      }

    } catch {
      case e: Exception => {
        logger.error("Something went wrong, exit")
        e.printStackTrace()
      }
    }
  }

  def readConf(file: String): RootConfig = {
    val userConfFile = new File(file)
    require(userConfFile.exists())

    ConfigFactory.invalidateCaches()
    System.setProperty("config.file", file)
    val userConf = ConfigFactory.load().getConfig("seqspark")

    /**
    val userConf = ConfigFactory
      .parseFile(userConfFile)
      .withFallback(ConfigFactory.load().getConfig("seqspark"))
      .resolve()
    */
    val show = userConf.root().render()

    val rootConfig = RootConfig(userConf)

    if (rootConfig.debug) {
      logger.debug("Conf detail:\n" + show)
    }

    rootConfig
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
      new File(conf.outDir).mkdir()
      true
    }
  }


  def run(cnf: RootConfig) {



    val user = System.getenv("USER")
    val hdfsHome = s"hdfs:///user/$user"

    val project = cnf.project

    /** Spark configuration */
    val scConf = new SparkConf().setAppName("SeqSpark-%s" format project)
    scConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    scConf.registerKryoClasses(Array(classOf[ConfigObject], classOf[Config], classOf[Bed], classOf[Var], classOf[Counter[(Double, Double)]]))
    val sc: SparkContext = new SparkContext(scConf)

    /** set checkpoint folder to hdfs home*/
    sc.setCheckpointDir(hdfsHome + "/checkpoint")

    val ss: SparkSession = SparkSession
      .builder()
      .appName("SeqSpark Phenotype")
      .config("spark.sql.warehouse", hdfsHome)
      .getOrCreate()

    Phenotype(cnf.input.phenotype.path, "phenotype")(ss)

    implicit val ssc = SingleStudyContext(cnf, sc, ss)

    /**
    if (cnf.pipeline.isEmpty) {
      logger.error("no pipeline specified, exit")
    } else if (cnf.pipeline.length > 2) {
      logger.error("too many tasks")
    } else if (cnf.pipeline.length == 1) {
      if (cnf.pipeline.head == "qualityControl") {
      }
    }
    */


    if (cnf.input.genotype.format == UserConfig.ImportGenotypeType.vcf) {
      val clean = try {
        val cachePath = cnf.input.genotype.path + s".$project"
        val res = sc.objectFile(cachePath).asInstanceOf[worker.Data[Byte]].map(identity)
        res.cache()
        logger.info(s"read from cache, rec: ${res.count()}")
        res
      } catch {
        case e: Exception =>
          logger.info("no cache, compute from start")
          val raw = Import.fromVCF(ssc)

          if (cnf.benchmark) {
            raw.cache()
            raw.foreach(_ => Unit)
            logger.info("VCF imported")
          }

          val res = QualityControl.cleanVCF(raw)
          res.cache()
          //res.saveAsObjectFile(cnf.project)
          res
      }
      runAssoc(clean)
    } else if (cnf.input.genotype.format == UserConfig.ImportGenotypeType.imputed) {
      val clean = try {
        val cachePath = cnf.input.genotype.path + s".$project"
        val res =sc.objectFile(cachePath).asInstanceOf[worker.Data[(Double, Double, Double)]].map(identity)
        res.cache()
        logger.info(s"read from cache, rec: ${res.count()}")
        res
      } catch {
        case e: Exception =>
          logger.info("no cache, compute from start")
          val res = QualityControl.cleanImputed(Import.fromImpute2(ssc))
          res.cache()
          if (cnf.benchmark) {
            res.foreach(_ => Unit)
            logger.info("quality control completed")
          }
          //res.saveAsObjectFile(cnf.project)
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

  /**
  def runImport[A, B](src: A, ssc: SingleStudyContext)(implicit imp: Import[A, B]): worker.Data[B] = {
    imp.load(ssc)
  }

  def runQC[A, B](input: worker.Data[A])(implicit qc: QualityControl[A, B], ssc: SingleStudyContext): worker.Data[B] = {
    qc.clean(input)
  }
*/
  def runAssoc[A: Genotype](input: worker.Data[A])
                           (implicit ssc: SingleStudyContext): Unit = {
    if (ssc.userConfig.pipeline.length == 2 || ssc.userConfig.pipeline.head == "association") {
      val assocConf = ssc.userConfig.association
      val methods = assocConf.methodList
      val annotated = if (methods.exists(m =>
        assocConf.method(m).misc.groupBy.contains("gene"))) {
        if (input.first().parseInfo.contains(util.Constant.Variant.InfoKey.anno)) {
          logger.info("functional annotation already done")
          input
        } else {
          logger.info("start functional annotation")
          annot.linkGeneDB(input)(ssc.userConfig, ssc.sparkContext)
        }
      } else {
        input
      }
      annotated.cache()
      if (ssc.userConfig.benchmark) {
        annotated.foreach(_ => Unit)
        logger.info("functional annotation completed")
      }
      //annotated.map(v => v.site).saveAsTextFile("test")
      val assoc = new AssocMaster(annotated)(ssc)
      assoc.run()
    }
  }



}

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
import org.dizhang.seqspark.util.{SeqContext, UserConfig}
import org.dizhang.seqspark.worker._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import java.nio.file.Files

/**
 * Main function
 */

object SingleStudy {

  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(seqContext: SeqContext) {

    /** quick run */

    try {

      if (checkConf(seqContext.userConfig)) {
        run(seqContext)
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

  def checkConf(conf: RootConfig): Boolean = {
    if (conf.pipeline.isEmpty) {
      logger.error("pipeline empty")
      false
    } else if (! Annotation.checkDB(conf.annotation)) {
      logger.error("conf error in annotation")
      false
    } else if (! conf.input.phenotype.pathValid && (conf.pipeline.length > 1 || conf.pipeline.head != "annotation")) {
      logger.error(s"phenotype input path not valid: ${conf.input.phenotype.pathRaw}")
      false
    } else if (! conf.input.genotype.pathValid) {
      logger.error(s"genotype input path not valid: ${conf.input.genotype.pathRaw}")
      false
    } else {
      //logger.info("Conf file fine")
      if (Files.exists(conf.output.results)) {
        if (Files.isDirectory(conf.output.results)) {
          logger.warn(s"using an existing output directory '${conf.output.results.toString}'")
          true
        } else {
          logger.error(s"'${conf.output.results.toString}' exists and is not a directory.")
          false
        }
      } else {
        logger.info(s"create output directory '${conf.output.results.toString}'")
        Files.createDirectories(conf.output.results)
        true
      }


    }
  }


  def run(seqContext: SeqContext) {



    val cnf = seqContext.userConfig

    val project = cnf.project

    val ss = seqContext.sparkSession

    val sc = seqContext.sparkContext

    Phenotype(cnf.input.phenotype, "phenotype")(ss)

    implicit val ssc = seqContext
    Pipeline(ssc)
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

    /**
    /** import genotype */
    val genoData: worker.GenoData = Import(ssc)

    val pipeline = cnf.pipeline

    val annotated = Annotation(genoData)

    if (cnf.input.genotype.format == UserConfig.GenotypeFormat.vcf) {
      val clean = try {
        val cachePath = cnf.input.genotype.path + s".$project"
        val res = sc.objectFile(cachePath).asInstanceOf[worker.Data[Byte]].map(identity)
        res.cache()
        logger.info(s"read from cache, rec: ${res.count()}")
        res
      } catch {
        case _: Exception =>
          logger.info("no cache, compute from start")
          val raw = Import.fromVCF(ssc)

          if (cnf.benchmark) {
            //raw.cache()
            //raw.checkpoint()
            //raw.foreach(_ => Unit)
            logger.info(s" ${raw.count()} variants from VCF imported")
          }

          val res = QualityControl.cleanVCF(raw)
          if (cnf.cache) res.cache()
          //res.saveAsObjectFile(cnf.project)
          res
      }
      runAssoc(clean)
    } else if (cnf.input.genotype.format == UserConfig.GenotypeFormat.imputed) {
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
*/
  }
  /**
  /**
  def runImport[A, B](src: A, ssc: SingleStudyContext)(implicit imp: Import[A, B]): worker.Data[B] = {
    imp.load(ssc)
  }

  def runQC[A, B](input: worker.Data[A])(implicit qc: QualityControl[A, B], ssc: SingleStudyContext): worker.Data[B] = {
    qc.clean(input)
  }
*/



  def runAssoc[A: Genotype](input: worker.Data[A])
                           (implicit ssc: SeqContext): Unit = {
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
*/


}

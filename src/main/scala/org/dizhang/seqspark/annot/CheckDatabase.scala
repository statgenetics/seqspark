package org.dizhang.seqspark.annot

import org.dizhang.seqspark.util.LogicalExpression
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by zhangdi on 9/25/16.
  */
object CheckDatabase {
  val logger = LoggerFactory.getLogger(this.getClass)

  def qcTermsInDB(conf: RootConfig): Boolean = {
    val qcVars = conf.qualityControl.variants
    val qcTerms = qcVars.flatMap(s => LogicalExpression.analyze(s)).toSet
    val dbRegex = """(\w+)\.(\w+)""".r
    val dbTerms = getDBterms(qcTerms).toArray.map{
      case dbRegex(db, field) => (db, field)
    }.groupBy(_._1).map(p => (p._1, p._2.map(_._2)))
    dbTerms.forall{
      case (db, fields) => checkDB(conf, db, fields)
      case x =>
        logger.warn(s"unrecognized terms in qualityControl.annotation.variants: $x")
        false
    }
  }
  def annTermsInDB(conf: RootConfig): Boolean = {
    val annConf = conf.annotation.variants
    if (annConf.hasPath("addInfo")) {
      val terms = annConf.getObject("addInfo").flatMap{
        case (k, v) => v.toString.split("/").map(_.replaceAll(" ", ""))
      }.map(_.split("\\.")).groupBy(_.apply(0)).map{
        case (k, v) => k -> v.flatten.toArray
      }
      terms.forall{
        case (db, fields) =>
          logger.info(s"$db: ${fields.mkString(",")}")
          checkDB(conf, db, fields)
      }
    } else {
      logger.info("no need to add info to VCF from databases")
      true
    }
  }

  def assTermsInDB(conf: RootConfig): Boolean = {
    val assConf = conf.association
    val methodList = assConf.methodList
    methodList.forall{m =>
      val mConf = assConf.method(m)
      if (mConf.misc.hasPath("variants")) {
        val assTerms = mConf.misc.getStringList("variants").asScala.toArray.flatMap(s =>
          LogicalExpression.analyze(s)
        ).toSet
        val dbRegex = """(\w+)\.(\w+)""".r
        val dbTerms = assTerms.filter{
          case dbRegex(_, _) => true
          case _ => false
        }
        dbTerms.map{case dbRegex(d, f) => (d, f)}.groupBy(_._1).map(p => (p._1, p._2.map(_._2).toArray)).forall{
          case (db, fields) => checkDB(conf, db, fields)
        }
      } else {
        logger.info(s"no variants selection in method $m")
        true
      }
    }
  }

  def getDBterms(names: Set[String]): Set[String] = {
    val fixedTerms = Set("alleleNum", "missingRate", "batchMissingRate", "batchSpecific", "hwePvalue", "isFunctional")
    names -- fixedTerms
  }


  def checkDB(conf: RootConfig, db: String, fields: Array[String] = Array[String]()): Boolean = {
    if (conf.annotation.config.hasPath(db)) {
      val dbConf = conf.annotation.config.getConfig(db)
      if (fields.isEmpty) {
        logger.info(s"database $db exists, no fields check required")
        true
      } else if (dbConf.getString("format") == "vcf") {
        logger.info(s"database $db is VCF, will not check fields, assuming in the INFO column")
        true
      } else if (dbConf.hasPath("header")) {
        if (fields.forall(f => dbConf.getStringList("header").contains(f))) {
          logger.info(s"database $db is plain file, check all fields with the provided header")
          true
        } else {
          logger.warn(s"database $db is plain file, not all fields found in header")
          false
        }
      } else {
        logger.info(s"database $db is plain file, will not check fields, assuming in the first line")
        true
      }
    } else {
      logger.warn(s"no database registry found in conf for $db")
      false
    }
  }
}

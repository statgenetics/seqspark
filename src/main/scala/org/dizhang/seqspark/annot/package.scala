package org.dizhang.seqspark

import java.io.{File, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqspark.annot.{IntervalTree, Location, RefGene, dbNSFP}
import org.dizhang.seqspark.ds._
import org.dizhang.seqspark.util.{Constant, LogicalExpression, SingleStudyContext}
import org.dizhang.seqspark.util.UserConfig.RootConfig
import org.dizhang.seqspark.worker.Data
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scalaz._

/**
  * Created by zhangdi on 8/15/16.
  */
package object annot {
  val logger = LoggerFactory.getLogger(getClass)

  def getDBs(conf: RootConfig): Map[String, Set[String]] = {
    val qcDBs = conf.qualityControl.variants.map{s => getDBs(s)}
    val annDBs = getDBs(conf.annotation.variants)
    logger.info(s"qcDBs: ${qcDBs.map{q => q.map{case (k,v) => s"$k,$v"}.mkString("-")}.mkString("|")}")
    logger.info(s"annDBs: ${annDBs.size}")

    if (conf.pipeline.last == "association") {
      val assDBs = conf.association.methodList.flatMap(m =>
        conf.association.method(m).misc.getStringList("variants").asScala.map{
          s => getDBs(s)
        }
      ).toList
      ((annDBs :: qcDBs) ::: assDBs).reduce((a, b) => addMap(a, b))
    } else {
      (annDBs :: qcDBs).reduce((a, b) => addMap(a, b))
    }
  }

  def addMap(x1: Map[String, Set[String]], x2: Map[String, Set[String]]) : Map[String, Set[String]] = {
    x1 ++ (for ((k, v) <- x2) yield if (x1.contains(k)) k -> (x1(k) ++ v) else k -> v)
  }

  def getDBs(cond: String): Map[String, Set[String]] = {
    LogicalExpression.analyze(cond).toArray.filter(n => n.contains(".")).map{n =>
      val s = n.split("\\.")
      (s(0), s(1))
    }.groupBy(_._1).map{
      case (k, v) => k -> v.map(_._2).toSet
    }
  }

  def getDBs(conf: Config): Map[String, Set[String]] = {
    conf.getObject("addInfo").map{
      case (k, v) =>
        val vs = v.toString.split("/").map(_.replaceAll(" ", ""))
            .map(_.split("."))
        k -> vs.filter(_.length > 1).map(_(1)).toSet
    }.toMap
  }

  def linkGeneDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link gene database ...")
    val dbConf = conf.annotation.RefSeq
    val build = "hg19"
    val coordFile = dbConf.getString("coord")
    val seqFile = dbConf.getString("seq")
    val refSeq = sc.broadcast(RefGene(build, coordFile, seqFile)(sc))
    input.map(v => v.annotateByVariant(refSeq))
  }

  def linkVariantDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link variant database ...")
    val dbTerms = getDBs(conf)
    logger.info(s"dbs: ${dbTerms.keys.mkString(",")}")
    var paired = input.map(v => (v.toVariation(), v))
    dbTerms.foreach{
      case (k, v) =>
        logger.info(s"join database ${k} with fields ${if (v.isEmpty) "None" else v.mkString(",")}")
        val db = VariantDB(conf.annotation.config.getConfig(k), v, conf.jobs)(sc)

        paired = paired.leftOuterJoin(db.info).map{
          case (va, (vt, Some(info))) =>
            vt.addInfo(k)
            db.header.zip(info).foreach{
              case (ik, iv) => vt.addInfo(ik, iv)}
            va -> vt
          case (va, (vt, None)) => va -> vt
        }
        //val cnt = paired.map{_.2}.reduce(_ + _)
        //logger.info(s"database $k annotated: $cnt")
    }
    paired.map(_._2)
  }
}

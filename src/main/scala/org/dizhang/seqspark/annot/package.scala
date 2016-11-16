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
    conf.getObject("addInfo").flatMap{
      case (k, v) => v.toString.split("/").map(_.replaceAll(" ", ""))
    }.filter(n => n.contains(".")).map(_.split("\\.")).groupBy(_.apply(0)).map{
      case (k, v) => k -> v.flatMap(r => r.slice(1, r.length)).toSet
    }
  }

  def linkGeneDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link gene database ...")
    val dbConf = conf.annotation.RefSeq
    val build = "hg19"
    val coordFile = dbConf.getString("coord")
    val seqFile = dbConf.getString("seq")
    val refSeq = sc.broadcast(RefGene(build, coordFile, seqFile)(sc))
    val annotated = input.map(v => v.annotateByVariant(refSeq))
    annotated.cache()
    val cnt = annotated.map(v =>
      if (v.parseInfo.contains(IK.anno)) {
        worstAnnotation(v.parseInfo(IK.anno)) -> 1
      } else {
        Anno.Feature.Unknown -> 1
      }).countByKey()
      .toArray.sortBy(p => FM(p._1))

    val pw = new PrintWriter(new File("output/annotation_summary.txt"))
    for ((k, v) <- cnt) {
      pw.write(s"${k.toString}: $v\n")
    }
    pw.close()
      //.aggregateByKey(0)(_ + _, _ + _).collect()
    annotated
  }

  def linkVariantDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link variant database ...")
    val dbTerms = getDBs(conf)
    val paired = input.map(v => (v.toVariation(), v))
    dbTerms.foreach{
      case (k, v) =>
        val db = VariantDB(conf.annotation.config.getConfig(k), v)(sc)
        paired.leftOuterJoin(db.info).map{
          case (va, (vt, Some(info))) => db.header.zip(info).foreach{
            case (ik, iv) =>  vt.addInfo(ik, iv)
          }
          case (va, (vt, None)) => {}
        }
    }
    input
  }
}

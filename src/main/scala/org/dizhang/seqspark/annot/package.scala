package org.dizhang.seqspark

import com.typesafe.config.Config
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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

  val CHRLEN = Array[Int](250, 244, 199, 191, 181, 172, 160, 147, 142, 136, 136,
    134, 116, 108, 103, 91, 82, 79, 60, 64,49, 52, 156, 60)

  object VariationPartitioner extends Partitioner {
    def numPartitions = CHRLEN.sum + 1
    def getPartition(key: Any): Int = key match {
      case Variation(chr, start, _, _, _, _) =>
        if (chr == 0) {
          CHRLEN.sum
        } else {
          CHRLEN.slice(0, chr - 1).sum + start/1000000
        }
      case _ => CHRLEN.sum
    }
  }

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
    val build = dbConf.getString("build")
    val coordFile = dbConf.getString("coordFile")
    val seqFile = dbConf.getString("seqFile")
    val refSeq = sc.broadcast(RefGene(build, coordFile, seqFile)(sc))
    input.map(v => v.annotateByVariant(refSeq))
  }

  def linkVariantDB[A](input: Data[A])(conf: RootConfig, sc: SparkContext): Data[A] = {
    logger.info("link variant database ...")
    val dbTerms = getDBs(conf)
    val paired = input.map(v => (v.toVariation(), v)).partitionBy(VariationPartitioner)
    dbTerms.foreach{
      case (k, v) =>
        val db = VariantDB(conf.annotation.config.getConfig(k), v)(sc)
        val info = db.info.partitionBy(VariationPartitioner)
        paired.leftOuterJoin(info).map{
          case (va, (vt, Some(i))) => db.header.zip(i).foreach{
            case (ik, iv) =>  vt.addInfo(ik, iv)
          }
          case (va, (vt, None)) => {}
        }
    }
    input
  }
}

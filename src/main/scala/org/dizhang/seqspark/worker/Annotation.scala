/*
 * Copyright 2018 Zhang Di
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

package org.dizhang.seqspark.worker

import org.apache.spark.SparkContext
import org.dizhang.seqspark.annot._
import org.dizhang.seqspark.annot.VariantAnnotOp._
import org.dizhang.seqspark.ds.{Genotype, Variant}
import org.dizhang.seqspark.util.{Constant, QueryParser, SeqContext}
import org.dizhang.seqspark.util.UserConfig._
import org.dizhang.seqspark.util.ConfigValue._
import org.slf4j.LoggerFactory
import org.apache.hadoop
import org.dizhang.seqspark.worker.Variants.countByFunction

object Annotation {

  private val logger = LoggerFactory.getLogger(getClass)

  private val dbExists = Constant.Variant.dbExists

  def apply[A: Genotype](data: Data[A], a: A)(implicit ssc: SeqContext): Data[A] = {
    logger.info("annotation")

    val conf = ssc.userConfig

    val queryExprs = QueryParser.parse(conf.annotation.addInfo)

    val dbs = QueryParser.dbs(queryExprs.values)

    val assocConf = conf.association

    /** need to perform functional annotation? */
    val geneAssoc = conf.pipeline.contains("association") && assocConf.methodList.exists(m =>
      assocConf.method(m).`type` match {
        case MethodType.meta | MethodType.snv => false
        case _ => true
      })

    val geneAnnot: Data[A] =
      if (dbs.contains("RefSeq") || geneAssoc) {
        val tmp = linkGeneDB(data)(conf, ssc.sparkContext)
        countByFunction(tmp)
        tmp
      } else {
        data
      }

    if (conf.pipeline.contains("annotation") ) {
      linkVariantDB(geneAnnot)(conf, ssc.sparkContext)
    } else {
      geneAnnot
    }
  }

  def checkDB(conf: AnnotationConfig): Boolean = {

    /** only consider undefined DB path as invalid */

    var valid = true

    val queryExprs = QueryParser.parse(conf.addInfo)
    val keys = queryExprs.keys.toList
    keys.foreach{k =>
      if (! k.matches("[a-zA-Z_]\\w+")) {
        logger.error(s"Please use a valid key name for seqspark.annotation.addInfo.$k")
      }
    }

    val dbKeys = QueryParser.dbKeys(queryExprs.values)

    dbKeys.foreach{
      case (db, fields) =>
        val dbConf = conf.db(db)
        if (dbConf.format == DBFormat.vcf) {
          logger.info("we don't check INFO.keys for VCF now")
        } else {
          /** check keys existance in db */
          val notIndb = fields -- dbConf.header - dbExists
          if (notIndb.nonEmpty) {
            logger.warn(s"keys: (${notIndb.mkString(" ")}) are not in db: $db")
          }
        }
        valid = valid && dbConf.pathValid
    }
    valid
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

    val queryExprs = QueryParser.parse(conf.annotation.addInfo)

    val dbTerms: Map[String, Set[String]] =
      QueryParser.dbKeys(queryExprs.values)
      .filter(_._1 != "RefSeq")

    if (dbTerms.isEmpty) {
      logger.info("no need to link variant databases")
      input
    } else {
      logger.info("link variant databases ...")
      logger.info(s"dbs: ${dbTerms.keys.mkString(",")}")
      var paired = input.map(v => (v.toVariation(), (v, Map[String, Map[String, String]]())))
      dbTerms.foreach{
        case (k, query) =>
          logger.info(s"join database $k with fields ${if (query.isEmpty) "None" else query.mkString(",")}")

          val db = VariantDB(conf.annotation.db(k), query, conf.partitions)(sc)

          paired = paired.leftOuterJoin(db.info).map{
            case (va, ((vt, old), Some(info))) =>
              (va, (vt, old.+(k -> db.header.zip(info).toMap)))
            case (va, ((vt, old), None)) => va -> vt
              (va, (vt, old))
          }
        //val cnt = paired.map{_.2}.reduce(_ + _)
        //logger.info(s"database $k annotated: $cnt")
      }
      /** now we can store the key-value pairs to the info field */
      paired.map{
        case (_, (vt, dbmap)) =>
          val res = QueryParser.eval(queryExprs)(dbmap)
          vt.updateInfo(res)
          vt
      }
    }

  }
}

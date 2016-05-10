package org.dizhang.seqspark.assoc

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats._
import com.typesafe.config.Config
import org.dizhang.seqspark.util.Constant.Pheno
import org.dizhang.seqspark.util.InputOutput._
import org.slf4j.{LoggerFactory, Logger}
import scala.annotation.tailrec


/**
 * Super class for association methods
 */

@SerialVersionUID(301L)
trait AssocMethod extends Serializable

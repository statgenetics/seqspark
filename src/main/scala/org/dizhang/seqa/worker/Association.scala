package org.dizhang.seqa.worker

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.dizhang.seqa.stat.LogisticRegression
import org.dizhang.seqa.util.InputOutput._
import java.util.logging
/**
 * Association testing
 */

object show {
  val genotypes = Array("0/0")

  /** Scala code demo
    * suppose genotypes is a collection
    * of genotypes */
  type Moc = (Int, Int)
  def moc(g: String) = g match {
    case "0/0" => (0, 2)
    case "0/1" => (1, 2)
    case "1/1" => (2, 2)
    case _ => (0, 0)
  }
  def compose(a: Moc, b: Moc) =
    (a._1 + b._1, a._2 + b._2)
  val mac = genotypes
    .map(g => moc(g))
    .reduce((a, b) => compose(a, b))


}

object Association extends Worker[VCF, VCF] {

  implicit val name = new WorkerName("association")

  def apply(input: VCF)(implicit cnf: Config, sc: SparkContext): VCF = {

    /** mute the netlib-java info level logging */
    val flogger = logging.Logger.getLogger("com.github.fommil")
    flogger.setLevel(logging.Level.WARNING)
    input
  }


  def cmc(vars: VCF)(implicit cnf: Config, sc: SparkContext): Unit = {
    val group = "ANNO"
    val filter = cnf.getStringList("association.filter")
    def make(b: Byte): Int = b.toInt
    val output = vars
      .filter(v => filter.contains(v.parseInfo("ANNO").split(":").last))
      .map(v => (v.parseInfo(group), v.toCounter(make)))
      .reduceByKey((a, b) => a ++ b)
      .map(x => (x._1, x._2.toDenseVector(x => if (x > 0) 1.0 else 0.0)))
    output.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val traits = cnf.getStringList("association.traits")
    val sampleInfo = cnf.getString("sampleInfo.source")
    for (tr <- traits.toArray) {
      if (tr == "Control") {
        val y = readColumn(sampleInfo, "Control")
        //if (cnf.getBoolean("association.permutation") == false)
          //output.map

      }
    }
  }
}

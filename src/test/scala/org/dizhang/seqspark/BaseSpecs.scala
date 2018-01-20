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

package org.dizhang.seqspark

import java.net.URL

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}
import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

import scala.sys.process._
import scala.io.Source
import scala.collection.JavaConverters._

object BaseSpecs {
  /** a short unit test for almost all unit tests */
  abstract class UnitSpec extends FlatSpec with Matchers with
    OptionValues with Inside with Inspectors {
    def logger: Logger = LoggerFactory.getLogger(getClass)
  }

  /**
    * Integration test setup
    * sparkContext
    *
  trait SharedSparkContext extends BeforeAndAfterAll { this: Suite =>
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(s"spark test")
      //.set("fs.defaultFS", "file://")

    var sc: SparkContext = _
    override def beforeAll(): Unit = {
      sc = new SparkContext(conf)
      super.beforeAll()
    }
    override def afterAll(): Unit = {
      try super.afterAll()
      finally sc.stop()
    }
  }
  */

  /** sparkSession */
  trait SharedSparkSession extends BeforeAndAfterAll { this: Suite =>

    var spark: SparkSession = _

    var sc: SparkContext = _

    override def beforeAll(): Unit = {
      spark = SparkSession.builder()
        .master("local")
        .appName("spark test")
        .getOrCreate()
      sc = spark.sparkContext
      super.beforeAll()
    }
    override def afterAll(): Unit = {
      try super.afterAll()
      finally spark.stop()
    }
  }

  abstract class IntegrationSpec extends UnitSpec with
    SharedSparkSession

  /**
    * In this shared test data trait
    * Check if dataset already exists
    * Check if it is updated
    * Otherwise download it
    * */
  trait SharedTestData extends BeforeAndAfterAll { this: Suite =>

    def logger: Logger
    def name: String = "test_data"
    private val files: List[String] = List(
      "test.dbsnp138.vcf.bz2",
      "test.gnomad.exome.vcf.bz2",
      "test.README",
      "test.refFlat_table",
      "test.refGene_seq",
      "test.tsv",
      "test.vcf.bz2"
    )
    private val url = "http://seqspark.statgen.us/data"
    private val rootFolder = Paths.get(getClass.getResource("/").getPath)
    private val dataFolder = rootFolder.resolve(name)
    private val md5file = rootFolder.resolve(s"$name.md5")

    def isDownloaded: Boolean = {
      Files.exists(md5file) && Files.exists(dataFolder) && files.forall(f => Files.exists(dataFolder.resolve(f)))
    }

    def isUpdated: Boolean = {

      /** load the local md5 into a map */
      val oldMd5: Map[String, String] =
        Source.fromFile(md5file.toFile).getLines().map{l =>
         val s = l.split("\\s+")
         s(1) -> s(0)
        }.toMap

      /** download the online md5 file */
      val newMd5file = rootFolder.resolve(s"$name.new.md5")

      /** load the online md5 file into a map */
      val newMd5: Map[String, String] =
        try {
          (new URL(s"$url/$name.md5") #>  newMd5file.toFile).!!

          Source.fromFile(newMd5file.toFile).getLines().map{l =>
            val s = l.split("\\s+")
            s(1) -> s(0)
          }.toMap
        } catch {
          case _: Exception =>
            logger.warn(s"cannot download $url/$name.md5")
            Map[String, String]()
        } finally Files.deleteIfExists(newMd5file)

      /** compare the two */
      newMd5.forall(p => oldMd5.contains(p._1) && oldMd5(p._1) == p._2)

    }

    def clear(): Unit = {
      Files.deleteIfExists(rootFolder.resolve(s"$name.new.md5"))
      Files.deleteIfExists(md5file)
      if (Files.exists(dataFolder)) {
        Files.walk(dataFolder)
          .iterator().asScala.toList.sorted(Ordering[Path].reverse)
          .foreach(p =>
            Files.deleteIfExists(p)
          )
        Files.deleteIfExists(dataFolder)
      }
    }

    def download(): Unit = {
      clear()
      (new URL(s"$url/$name.md5") #> rootFolder.resolve(s"$name.md5").toFile).!!
      if (! Files.exists(dataFolder)) Files.createDirectories(dataFolder)
      files.foreach(f =>
        (new URL(s"$url/$name/$f") #> dataFolder.resolve(s"$f").toFile).!!
      )
    }

    override def beforeAll(): Unit = {

      if (! (isDownloaded && isUpdated)) {
        logger.info(s"going to download test dataset: $name")
        download()
      } else {
        logger.info("no need to download test dataset")
      }
      super.beforeAll()
    }

  }

}

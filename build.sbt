name := "seqspark"

version := "1.0"

organization := "org.dizhang"

scalaVersion := "2.10.6"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies ++= Seq(
	"org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
	"org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided",
	"org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
	"org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided",
	"org.apache.spark" % "spark-mllib_2.10" % "1.6.0" % "provided",
	"com.typesafe" % "config" % "1.2.1",
	"it.unimi.dsi" % "fastutil" % "7.0.4",
	"org.scalanlp" %% "breeze" % "0.12",
	"org.scalanlp" %% "breeze-natives" % "0.12",
	"org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
	"net.java.dev.jna" % "jna" % "4.2.2",
	"org.scalaz" %% "scalaz-core" % "7.2.4"
)

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := "SeqSpark.jar"

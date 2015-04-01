name := "test"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"
libraryDependencies += "org.ini4j" % "ini4j" % "0.5.4"
//libraryDependencies += "org.ini4j" %% "ini4j" % "0.5.2"

//libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.1"

ThisBuild / javacOptions ++= Seq("--release", "11")
ThisBuild / scalacOptions += "-target:11"

lazy val root = (project in file("."))
  .settings(
    name := "scala_big_data_core"
  )

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"

// STTP
libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M18"
libraryDependencies += "com.softwaremill.sttp.client4" %% "okhttp-backend" % "4.0.0-M18"

// JSON
libraryDependencies += "com.lihaoyi" %% "upickle" % "4.0.2"
libraryDependencies += "com.lihaoyi" %% "fansi" % "0.5.0"

// PureConfig
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.8"

// AWS SDK v2
libraryDependencies += "software.amazon.awssdk" % "s3" % "2.39.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"



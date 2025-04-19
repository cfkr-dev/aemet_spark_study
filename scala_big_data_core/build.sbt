ThisBuild / javacOptions ++= Seq("--release", "11")
ThisBuild / scalacOptions += "-target:11"

lazy val root = (project in file("."))
  .settings(
    name := "scala_big_data_core"
  )

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"

// https://mvnrepository.com/artifact/com.softwaremill.sttp.client4/core
libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M18"

// https://mvnrepository.com/artifact/com.lihaoyi/upickle
libraryDependencies += "com.lihaoyi" %% "upickle" % "4.0.2"

// https://mvnrepository.com/artifact/com.lihaoyi/fansi
libraryDependencies += "com.lihaoyi" %% "fansi" % "0.5.0"

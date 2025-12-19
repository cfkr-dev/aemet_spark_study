ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

// ----------------------
//     MERGE STRATEGY
// ----------------------

ThisBuild / assemblyMergeStrategy := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.first
  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.last
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x if x.endsWith("descriptor.proto") => MergeStrategy.discard
  case x if x.endsWith("arrow-git.properties") => MergeStrategy.discard
  case x if x.endsWith("module-info.class") => MergeStrategy.last
  case x if x.endsWith("AuthenticationType.class") => MergeStrategy.last
  case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.last
  case x if x.endsWith(".kotlin_module") => MergeStrategy.discard
  case x if x.contains("FastDoubleParser-NOTICE") => MergeStrategy.discard
  case x if x.endsWith("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.first
  case x if x.endsWith("FastDoubleParser-LICENSE") => MergeStrategy.discard
  case "git.properties" => MergeStrategy.discard
  case "mime.types" => MergeStrategy.last
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// ------------
//     ROOT
// ------------
lazy val root = (project in file("."))
  .aggregate(utils, sparkApp)
  .settings(
    name := "big-data-core-emr",
    publish / skip := true,
    assembly / skip := true
  )

// -----------------
//     UTILS LIB
// -----------------
lazy val utils = (project in file("modules/utils"))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "utils",
    libraryDependencies ++= Seq(
      // UPICKLE
      "com.lihaoyi" %% "upickle" % "4.0.2",

      // FANSI
      "com.lihaoyi" %% "fansi" % "0.5.0",

      // PURECONFIG
      "com.github.pureconfig" %% "pureconfig" % "0.17.8",

      // AWS SDK
      "software.amazon.awssdk" % "s3" % "2.31.16" % Provided
    ),
    assembly / assemblyJarName := "utils-1.0.0.jar"
  )

// -------------
//     SPARK
// -------------
lazy val sparkApp = (project in file("modules/spark"))
  .dependsOn(utils)
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "spark",
    mainClass := Some("Spark.Main"),
    libraryDependencies ++= Seq(
      // SPARK
      "org.apache.spark" %% "spark-core" % "3.5.5" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.5" % Provided,

      // HADOOP AWS
      "org.apache.hadoop" % "hadoop-aws" % "3.4.1" % Provided
    ),
    assembly / assemblyJarName := "spark-app-cluster-1.0.0.jar"
  )




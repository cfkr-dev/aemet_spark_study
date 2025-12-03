ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.15"

// ----------------------
//  ROOT
// ----------------------
lazy val root = (project in file("."))
  .aggregate(utils, dataExtraction, sparkApp, plotGeneration)
  .settings(
    name := "big-data-core",
    publish / skip := true
  )

// ----------------------
//  UTILS (lib compartida)
// ----------------------
lazy val utils = (project in file("modules/utils"))
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "utils",
    libraryDependencies ++= Seq(
      // STTP CLIENT 4
      "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M18",
      "com.softwaremill.sttp.client4" %% "okhttp-backend" % "4.0.0-M18",

      // UPICKLE
      "com.lihaoyi" %% "upickle" % "4.0.2",

      // FANSI
      "com.lihaoyi" %% "fansi" % "0.5.0",

      // PURECONFIG
      "com.github.pureconfig" %% "pureconfig" % "0.17.8",

      // AWS SDK
      "software.amazon.awssdk" % "s3" % "2.39.1"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("descriptor.proto") => MergeStrategy.discard
      case x if x.endsWith("arrow-git.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.last
      case x if x.endsWith("AuthenticationType.class") => MergeStrategy.last
      case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.last
      case "git.properties" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

// ----------------------
//  DATA EXTRACTION
// ----------------------
lazy val dataExtraction = (project in file("modules/data-extraction"))
  .dependsOn(utils)
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "data-extraction",
    mainClass := Some("DataExtraction.Main"),
    libraryDependencies ++= Seq(),
    assembly / assemblyMergeStrategy := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("descriptor.proto") => MergeStrategy.discard
      case x if x.endsWith("arrow-git.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.last
      case x if x.endsWith("AuthenticationType.class") => MergeStrategy.last
      case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.last
      case "git.properties" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

// ----------------------
//  PLOT GENERATION
// ----------------------
lazy val plotGeneration = (project in file("modules/plot-generation"))
  .dependsOn(utils)
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "plot-generation",
    mainClass := Some("PlotGeneration.Main"),
    libraryDependencies ++= Seq(),
    assembly / assemblyMergeStrategy := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("descriptor.proto") => MergeStrategy.discard
      case x if x.endsWith("arrow-git.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.last
      case x if x.endsWith("AuthenticationType.class") => MergeStrategy.last
      case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.last
      case "git.properties" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )


// ----------------------
//  SPARK
// ----------------------
lazy val sparkApp = (project in file("modules/spark"))
  .dependsOn(utils)
  .enablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "spark",
    mainClass := Some("Spark.Main"),
    libraryDependencies ++= Seq(
      // SPARK
      "org.apache.spark" %% "spark-core" % "3.5.3" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.3" % Provided,
      //"org.apache.spark" %% "spark-core" % "3.5.3", -- Uncomment for local execution (comment the up one)
      //"org.apache.spark" %% "spark-sql" % "3.5.3",  -- Uncomment for local execution (comment the up one)

      // HADOOP AWS
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("google", "protobuf", xs @ _*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("descriptor.proto") => MergeStrategy.discard
      case x if x.endsWith("arrow-git.properties") => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.last
      case x if x.endsWith("AuthenticationType.class") => MergeStrategy.last
      case x if x.endsWith("Log4j2Plugins.dat") => MergeStrategy.last
      case "git.properties" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )




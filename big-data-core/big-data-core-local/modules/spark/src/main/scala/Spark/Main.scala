package Spark

import Spark.Core.SparkManager

/**
 * Application entry point for the project.
 *
 * This object extends `App`, so the body is executed on JVM startup. It delegates
 * execution to the `SparkManager` which coordinates the Spark session and configured
 * studies/queries.
 *
 * Side effects: starts Spark jobs and performs configured data processing tasks.
 */
object Main extends App {
  SparkManager.execute()
}

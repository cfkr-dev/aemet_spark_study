package Spark

import Spark.Core.SparkManager

/**
 * Main application entry point.
 *
 * This object extends `App` and invokes `Spark.Core.SparkManager.SparkQueries.execute()`
 * to start the full set of query-driven studies (stations, climographs,
 * single-parameter studies and interesting studies). It serves as a minimal
 * runner used when launching the Spark module.
 */
object Main extends App {
  SparkManager.execute()
}

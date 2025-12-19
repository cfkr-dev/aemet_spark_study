package Spark.Core.Queries

import Spark.Core.Session.SparkSessionCore

/**
 * SparkQueries groups query-oriented workflows that produce final datasets
 * (studies, plots, and aggregations) based on the preformatted DataFrames in
 * `SparkCore.dataframes`.
 *
 * Use `execute()` to run the full set of queries in sequence (it starts and
 * stops the Spark session around the set of studies).
 */
object SparkQueriesManager {
  def createSparkQueriesCore(sparkSessionCore: SparkSessionCore): SparkQueriesCore = {
    SparkQueriesCore(sparkSessionCore)
  }
}
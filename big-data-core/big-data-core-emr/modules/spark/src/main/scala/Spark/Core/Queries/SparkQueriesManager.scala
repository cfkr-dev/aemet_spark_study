package Spark.Core.Queries

import Spark.Core.Session.SparkSessionCore

/**
 * Factory object for creating `SparkQueriesCore` instances.
 *
 * This small helper centralizes instantiation of the queries core and makes
 * calling code concise (avoids repeating the case class constructor call).
 */
object SparkQueriesManager {
  /**
   * Create a `SparkQueriesCore` instance bound to the provided Spark session core.
   *
   * @param sparkSessionCore the helper that contains the Spark session and context
   * @return a new `SparkQueriesCore` instance that uses the provided session
   */
  def createSparkQueriesCore(sparkSessionCore: SparkSessionCore): SparkQueriesCore = {
    SparkQueriesCore(sparkSessionCore)
  }
}
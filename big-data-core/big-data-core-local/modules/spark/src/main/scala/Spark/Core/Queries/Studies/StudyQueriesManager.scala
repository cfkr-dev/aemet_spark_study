package Spark.Core.Queries.Studies

import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore

/**
 * Generic factory for instantiating study-specific `StudyQueriesCore` implementations.
 *
 * This helper accepts a creator function that constructs a concrete subtype of
 * `StudyQueriesCore` given a `SparkSessionCore` and a `SparkQueriesCore`. It
 * simplifies calling code by encapsulating the creation pattern used across the
 * application.
 */
object StudyQueriesManager {
  /**
   * Generic factory that creates a concrete `StudyQueriesCore` implementation.
   *
   * This helper accepts a creator function and applies it to the provided
   * `SparkSessionCore` and `SparkQueriesCore` so callers can instantiate
   * specific study query cores concisely.
   *
   * @param sparkSessionCore the Spark session helper to pass to the creator
   * @param sparkQueriesCore the queries helper to pass to the creator
   * @param creator a function that constructs a concrete `StudyQueriesCore` given the helpers
   * @tparam T specific subtype of `StudyQueriesCore` to create
   * @return an instance of `T` constructed by the provided creator
   */
  def createStudyQueriesCore[T <: StudyQueriesCore](
    sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore
  )(
    creator: (SparkSessionCore, SparkQueriesCore) => T
  ): T = {
    creator(sparkSessionCore, sparkQueriesCore)
  }
}

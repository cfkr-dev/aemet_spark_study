package Spark.Core

import Spark.Core.Queries.Studies.{ClimographsQueriesCore, InterestingStudiesQueriesCore, SingleParamStudiesQueriesCore, StationsQueriesCore, StudyQueriesManager}
import Spark.Core.Queries.{SparkQueriesCore, SparkQueriesManager}
import Spark.Core.Session.{SparkSessionCore, SparkSessionManager}

/**
 * Top-level orchestrator that prepares the Spark session and runs configured studies.
 *
 * Responsibilities:
 * - Creates a `SparkSessionCore` instance using `SparkSessionManager`.
 * - Initializes `SparkQueriesCore` and each study-specific queries core using
 *   `StudyQueriesManager` and the corresponding constructors.
 * - Executes each study in sequence and finally ends the Spark session.
 */
object SparkManager {
  /**
   * Orchestrates the execution of all configured studies.
   *
   * Behaviour:
   * - Creates and starts the Spark session.
   * - Instantiates query cores and study cores.
   * - Executes each study in a predefined order (stations, climographs, single-param,
   *   and interesting studies).
   * - Stops the Spark session when all studies have finished.
   *
   * Side effects: starts and stops the Spark session and triggers many Spark jobs
   * which persist output to configured storage locations.
   */
  def execute(): Unit = {
    val sparkSessionCore: SparkSessionCore = SparkSessionManager.createSparkSessionCore()

    sparkSessionCore.startSparkSession()

    val sparkQueriesCore: SparkQueriesCore = SparkQueriesManager.createSparkQueriesCore(sparkSessionCore)

    val stationsQueries: StationsQueriesCore = StudyQueriesManager.createStudyQueriesCore(
      sparkSessionCore, sparkQueriesCore
    )(
      StationsQueriesCore(_, _)
    )
    stationsQueries.execute()

    val climographsQueries: ClimographsQueriesCore = StudyQueriesManager.createStudyQueriesCore(
      sparkSessionCore, sparkQueriesCore
    )(
      ClimographsQueriesCore(_, _)
    )
    climographsQueries.execute()

    val singleParamStudiesQueries: SingleParamStudiesQueriesCore = StudyQueriesManager.createStudyQueriesCore(
      sparkSessionCore, sparkQueriesCore
    )(
      SingleParamStudiesQueriesCore(_, _)
    )
    singleParamStudiesQueries.execute()

    val interestingStudiesQueries: InterestingStudiesQueriesCore = StudyQueriesManager.createStudyQueriesCore(
      sparkSessionCore, sparkQueriesCore
    )(
      InterestingStudiesQueriesCore(_, _)
    )
    interestingStudiesQueries.execute()


    sparkSessionCore.endSparkSession()
  }
}
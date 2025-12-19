package Spark.Core

import Spark.Core.Queries.Studies.{ClimographsQueriesCore, InterestingStudiesQueriesCore, SingleParamStudiesQueriesCore, StationsQueriesCore, StudyQueriesManager}
import Spark.Core.Queries.{SparkQueriesCore, SparkQueriesManager}
import Spark.Core.Session.{SparkSessionCore, SparkSessionManager}

object SparkManager {
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
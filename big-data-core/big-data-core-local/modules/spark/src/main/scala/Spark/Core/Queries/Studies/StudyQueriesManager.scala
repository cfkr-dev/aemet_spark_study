package Spark.Core.Queries.Studies

import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore

object StudyQueriesManager {
  def createStudyQueriesCore[T <: StudyQueriesCore](
    sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore
  )(
    creator: (SparkSessionCore, SparkQueriesCore) => T
  ): T = {
    creator(sparkSessionCore, sparkQueriesCore)
  }
}

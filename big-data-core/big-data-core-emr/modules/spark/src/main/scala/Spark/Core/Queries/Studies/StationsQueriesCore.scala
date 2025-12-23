package Spark.Core.Queries.Studies

import Spark.Config.SparkConf
import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Core queries implementation for station-focused studies.
 *
 * This case class executes queries that analyze station counts and distributions
 * (for example counts by state or altitude intervals) and persists the results.
 *
 * @param sparkSessionCore helper containing the Spark session and context utilities
 * @param sparkQueriesCore component that exposes reusable Spark query functions
 */
case class StationsQueriesCore(sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore)
  extends StudyQueriesCore(sparkSessionCore) {

  private val ctsExecution = SparkConf.Constants.queries.execution.stationsConf
  private val ctsStorage = SparkConf.Constants.queries.storage.stationsConf
  private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf
  private val ctsLogs = SparkConf.Constants.queries.log.stationsConf

  /**
   * Execute the Stations study: run station-related queries and persist results.
   *
   * The method uses `simpleFetchAndSave` to orchestrate fetching, displaying
   * and saving DataFrames produced by the `sparkQueriesCore` helpers. Errors
   * returned by query helpers are logged as warnings and skipped.
   */
  def execute(): Unit = {
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
      ctsLogs.studyName
    ))

    simpleFetchAndSave(
      ctsLogs.stationCountEvolFromStart,
      List(
        FetchAndSaveInfo(
          sparkQueriesCore.getStationCountByColumnInLapse(
            column = (
              year(col(ctsExecution.countEvolFromStart.param)),
              ctsExecution.countEvolFromStart.paramSelectName
            ),
            startDate = ctsExecution.countEvolFromStart.startDate,
            endDate = Some(ctsExecution.countEvolFromStart.endDate)) match {
            case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
              return
            case Right(dataFrame: DataFrame) => dataFrame
          },
          ctsStorage.countEvolFromStart.data
        )
      )
    )

    simpleFetchAndSave(
      ctsLogs.stationCountByState2024,
      List(
        FetchAndSaveInfo(
          sparkQueriesCore.getStationCountByColumnInLapse(
            column = (
              col(ctsExecution.countByState2024.param),
              ctsExecution.countByState2024.paramSelectName
            ),
            startDate = ctsExecution.countByState2024.startDate) match {
            case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
              return
            case Right(dataFrame: DataFrame) => dataFrame
          },
          ctsStorage.countByState2024.data
        )
      )
    )

    simpleFetchAndSave(
      ctsLogs.stationCountByAltitude2024,
      List(
        FetchAndSaveInfo(
          sparkQueriesCore.getStationsCountByParamIntervalsInALapse(
            paramIntervals = ctsExecution.countByAltitude2024.intervals,
            startDate = ctsExecution.countByAltitude2024.startDate) match {
            case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
              return
            case Right(dataFrame: DataFrame) => dataFrame
          },
          ctsStorage.countByAltitude2024.data
        )
      )
    )

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
      ctsLogs.studyName
    ))
  }
}

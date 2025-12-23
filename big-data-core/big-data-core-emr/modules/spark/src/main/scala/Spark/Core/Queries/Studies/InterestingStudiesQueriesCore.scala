package Spark.Core.Queries.Studies

import Spark.Config.SparkConf
import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.sql.DataFrame

/**
 * Core queries implementation for multi-parameter "interesting" studies.
 *
 * This case class provides the logic to run configured multi-parameter analyses
 * (for example, precipitation and pressure evolutions) and to persist the
 * resulting DataFrames to the configured storage locations.
 *
 * @param sparkSessionCore helper containing the Spark session and context utilities
 * @param sparkQueriesCore component that exposes reusable Spark query functions
 */
case class InterestingStudiesQueriesCore(sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore)
  extends StudyQueriesCore(sparkSessionCore) {

  private val ctsExecution = SparkConf.Constants.queries.execution.interestingStudiesConf
  private val ctsStorage = SparkConf.Constants.queries.storage.interestingStudiesConf
  private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf
  private val ctsLogs = SparkConf.Constants.queries.log.interestingStudiesConf


  /**
   * Execute the set of interesting multi-parameter studies and persist results.
   *
   * Behaviour:
   * - Runs queries defined in configuration for each configured record and top-N sets.
   * - For each configured record it fetches station info, climate parameter evolutions,
   *   and yearly grouped aggregates as required by the study.
   * - Persists results and logs progress. When query methods return `Left(exception)`
   *   a warning is logged and the failing fetch is skipped.
   */
  def execute(): Unit = {
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
      ctsLogs.studyName
    ))

    simpleFetchAndSave(
      ctsLogs.precAndPressureEvolFromStartForEachState,
      ctsExecution.stationRecords.flatMap(record => {
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getStationInfoById(record.stationIdGlobal) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.precAndPressEvol.dataStationGlobal.format(
              record.stateNameNoSc
            ),
            ctsLogs.precAndPressureEvolFromStartForEachStateStartStationGlobal.format(
              record.stateName.capitalize
            ),
            saveAsJSON = true
          ),
          FetchAndSaveInfo(
            sparkQueriesCore.getStationInfoById(record.stationIdLatest) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.precAndPressEvol.dataStationLatest.format(
              record.stateNameNoSc
            ),
            ctsLogs.precAndPressureEvolFromStartForEachStateStartStationLatest.format(
              record.stateName.capitalize
            ),
            saveAsJSON = true
          ),
          FetchAndSaveInfo(
            sparkQueriesCore.getClimateParamInALapseById(
              record.stationIdLatest,
              ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
              ctsExecution.precAndPressEvolFromStartForEachState.colAggMethods,
              record.startDateLatest,
              Some(record.endDateLatest)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.precAndPressEvol.dataEvol.format(
              record.stateNameNoSc
            ),
            ctsLogs.precAndPressureEvolFromStartForEachStateStartEvol.format(
              record.stateName
            )
          ),
          FetchAndSaveInfo(
            sparkQueriesCore.getClimateYearlyGroupById(
              record.stationIdGlobal,
              ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
              ctsExecution.precAndPressEvolFromStartForEachState.colAggMethods,
              record.startDateGlobal,
              Some(record.endDateGlobal)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.precAndPressEvol.dataEvolYearlyGroup.format(
              record.stateNameNoSc
            ),
            ctsLogs.precAndPressureEvolFromStartForEachStateYearlyGroup.format(
              record.stateName
            )
          ),
        )
      })
    )

    ctsExecution.top10States.foreach(top10 => {
      simpleFetchAndSave(
        ctsLogs.top10States.format(top10.name),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateConditionsInALapse(
              climateParams = top10.climateParams,
              startDate = top10.startDate,
              endDate = Some(top10.endDate),
              groupByState = true
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10States.dataTop.format(top10.nameAbbrev)
          )
        )
      )
    })

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
      ctsLogs.studyName
    ))
  }
}

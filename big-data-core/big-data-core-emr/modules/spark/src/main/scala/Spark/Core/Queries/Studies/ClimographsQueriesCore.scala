package Spark.Core.Queries.Studies

import Spark.Config.SparkConf
import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.sql.DataFrame

/**
 * Core queries implementation for the Climograph study.
 *
 * This case class bundles the dependencies required to run the climograph-related
 * queries and persist their outputs. It extends `StudyQueriesCore` to reuse
 * common fetching and saving utility behaviour.
 *
 * @param sparkSessionCore helper containing the Spark session and context utilities
 * @param sparkQueriesCore component that exposes reusable Spark query functions
 */
case class ClimographsQueriesCore(sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore)
  extends StudyQueriesCore(sparkSessionCore) {

  private val ctsExecution = SparkConf.Constants.queries.execution.climographConf
  private val ctsStorage = SparkConf.Constants.queries.storage.climographConf
  private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf
  private val ctsLogs = SparkConf.Constants.queries.log.climographConf

  /**
   * Execute the Climograph study: produce station-level and monthly aggregates
   * used for climographs and persist the outputs.
   *
   * Behaviour:
   * - Iterates over configured climate groups and their climates.
   * - For each climate location, it fetches station info and computes monthly
   *   temperature averages and precipitation sums for the configured observation year.
   * - Saves results to configured storage paths (JSON and/or tables) and logs progress.
   *
   * Notes on error handling: when a query method returns `Left(exception)`, a warning
   * is logged and the current fetch/save operation is skipped for that record.
   */
  def execute(): Unit = {
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
      ctsLogs.studyName
    ))

    ctsExecution.stationsRecords.foreach(climateGroup => {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.startFetchingClimateGroup.format(
        climateGroup.climateGroupName
      ), encloseHalfLength = 35)

      climateGroup.climates.foreach(climateRecord => {
        simpleFetchAndSave(
          ctsLogs.fetchingClimate.format(
            climateRecord.climateName
          ),
          climateRecord.records.flatMap(record => {
            List(
              FetchAndSaveInfo(
                sparkQueriesCore.getStationInfoById(record.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.climograph.dataStation.format(
                  climateGroup.climateGroupName,
                  climateRecord.climateName,
                  record.location.replace(" ", "_")
                ),
                ctsLogs.fetchingClimateLocationStation.format(
                  record.location.capitalize,
                ),
                saveAsJSON = true
              ),
              FetchAndSaveInfo(
                sparkQueriesCore.getStationMonthlyAvgTempAndSumPrecInAYear(
                  record.stationId,
                  (ctsExecution.studyParamNames.temperature, ctsExecution.studyParamNames.precipitation),
                  ctsExecution.observationYear
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.climograph.dataTempAndPrec.format(
                  climateGroup.climateGroupName,
                  climateRecord.climateName,
                  record.location.replace(" ", "_")
                ),
                ctsLogs.fetchingClimateLocationTempPrec.format(
                  record.location.capitalize,
                )
              )
            )
          }),
          encloseHalfLengthStart = 40
        )
      })
    })

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
      ctsLogs.studyName
    ))
  }
}

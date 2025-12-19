package Spark.Core.Queries.Studies

import Spark.Config.SparkConf
import Spark.Core.Queries.SparkQueriesCore
import Spark.Core.Session.SparkSessionCore
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

case class SingleParamStudiesQueriesCore(sparkSessionCore: SparkSessionCore, sparkQueriesCore: SparkQueriesCore)
  extends StudyQueriesCore(sparkSessionCore) {

  private val ctsExecution = SparkConf.Constants.queries.execution.singleParamStudiesConf
  private val ctsStorage = SparkConf.Constants.queries.storage.singleParamStudiesConf
  private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf
  private val ctsLogs = SparkConf.Constants.queries.log.singleParamStudiesConf

  /**
   * Execute the configured single-parameter studies (top-N, evolutions, etc.).
   */
  def execute(): Unit = {
    ctsExecution.singleParamStudiesValues.foreach(study => {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
        study.studyParam.replace("_", " ")
      ))

      simpleFetchAndSave(
        ctsLogs.top10Highest2024.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10Highest2024.startDate,
              endDate = None) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataHighest2024.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top10HighestDecade.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10HighestDecade.startDate,
              endDate = Some(ctsExecution.top10HighestDecade.endDate)) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataHighestDecade.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top10HighestGlobal.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10HighestGlobal.startDate,
              endDate = Some(ctsExecution.top10HighestGlobal.endDate)) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataHighestGlobal.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top10Lowest2024.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10Lowest2024.startDate,
              endDate = None,
              highest = false
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataLowest2024.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top10LowestDecade.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10LowestDecade.startDate,
              endDate = Some(ctsExecution.top10LowestDecade.endDate),
              highest = false
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataLowestDecade.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top10LowestGlobal.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.top10LowestGlobal.startDate,
              endDate = Some(ctsExecution.top10LowestGlobal.endDate),
              highest = false
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top10.dataLowestGlobal.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      val regressionModelDf: DataFrame = simpleFetchAndSave(
        ctsLogs.evolFromStartForEachState.format(
          study.studyParam.capitalize
        ),
        study.reprStationRegs.flatMap(record => {
          List(
            FetchAndSaveInfo(
              sparkQueriesCore.getStationInfoById(record.stationIdGlobal) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.evolFromStartForEachState.dataStationGlobal.format(
                study.studyParamAbbrev,
                record.stateNameNoSc
              ),
              ctsLogs.evolFromStartForEachStateStartStationGlobal.format(
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
              ctsStorage.evolFromStartForEachState.dataStationLatest.format(
                study.studyParamAbbrev,
                record.stateNameNoSc
              ),
              ctsLogs.evolFromStartForEachStateStartStationLatest.format(
                record.stateName.capitalize
              ),
              saveAsJSON = true
            ),
            FetchAndSaveInfo(
              sparkQueriesCore.getClimateParamInALapseById(
                record.stationIdLatest,
                List(
                  (study.dataframeColName, study.studyParamAbbrev)
                ),
                List(study.colAggMethod),
                record.startDateLatest,
                Some(record.endDateLatest)
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.evolFromStartForEachState.dataEvol.format(
                study.studyParamAbbrev,
                record.stateNameNoSc
              ),
              ctsLogs.evolFromStartForEachStateStart.format(
                record.stateName,
                study.studyParam.replace("_", " ")
              )
            ),
            FetchAndSaveInfo(
              sparkQueriesCore.getClimateYearlyGroupById(
                record.stationIdGlobal,
                List(
                  (study.dataframeColName, study.studyParamAbbrev)
                ),
                List(study.colAggMethod),
                record.startDateGlobal,
                Some(record.endDateGlobal)
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.evolFromStartForEachState.dataEvolYearlyGroup.format(
                study.studyParamAbbrev,
                record.stateNameNoSc
              ),
              ctsLogs.evolFromStartForEachStateYearlyGroup.format(
                record.stateName,
                study.studyParam.replace("_", " "),
                study.colAggMethod
              )
            ),
            FetchAndSaveInfo(
              sparkQueriesCore.getStationClimateParamRegressionModelInALapse(
                record.stationIdGlobal,
                study.dataframeColName,
                study.colAggMethod,
                record.startDateGlobal,
                Some(record.endDateGlobal)
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.evolFromStartForEachState.dataEvolRegression.format(
                study.studyParamAbbrev,
                record.stateNameNoSc
              ),
              ctsLogs.evolFromStartForEachStateStartRegression.format(
                record.stateName.capitalize,
                study.studyParam.replace("_", " ")
              )
            )
          )
        })
      ).zipWithIndex.filter {
        case (_, idx) => idx >= 4 && (idx - 4) % 5 == 0
      }.map(_._1).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK_SER)

      regressionModelDf.count()

      simpleFetchAndSave(
        ctsLogs.top5HighestInc.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamIncrementInAYearLapse(
              stationIds = study.reprStationRegs.map(record => record.stationIdGlobal),
              regressionModels = regressionModelDf,
              climateParam = study.dataframeColName,
              paramNameToShow = study.studyParamAbbrev,
              aggMethodName = study.colAggMethod,
              startYear = ctsExecution.top5HighestInc.startYear,
              endYear = ctsExecution.top5HighestInc.endYear
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top5Inc.dataHighest.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.top5LowestInc.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getTopNClimateParamIncrementInAYearLapse(
              stationIds = study.reprStationRegs.map(record => record.stationIdGlobal),
              regressionModels = regressionModelDf,
              climateParam = study.dataframeColName,
              paramNameToShow = study.studyParamAbbrev,
              aggMethodName = study.colAggMethod,
              startYear = ctsExecution.top5LowestInc.startYear,
              endYear = ctsExecution.top5LowestInc.endYear,
              highest = false
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.top5Inc.dataLowest.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      regressionModelDf.unpersist()

      simpleFetchAndSave(
        ctsLogs.avg2024AllStationSpainContinental.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getAllStationsByStatesAvgClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.avg2024AllStationSpain.startDate,
              states = Some(ctsExecution.avg2024AllStationSpain.continentalStates)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.avg2024AllStationsSpain.dataContinental.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      simpleFetchAndSave(
        ctsLogs.avg2024AllStationSpainCanary.format(
          study.studyParam
        ),
        List(
          FetchAndSaveInfo(
            sparkQueriesCore.getAllStationsByStatesAvgClimateParamInALapse(
              climateParam = study.dataframeColName,
              aggMethodName = study.colAggMethod,
              paramNameToShow = study.studyParamAbbrev,
              startDate = ctsExecution.avg2024AllStationSpain.startDate,
              states = Some(ctsExecution.avg2024AllStationSpain.canaryIslandStates)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                return
              case Right(dataFrame: DataFrame) => dataFrame
            },
            ctsStorage.avg2024AllStationsSpain.dataCanary.format(
              study.studyParamAbbrev
            )
          )
        )
      )

      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
        study.studyParam
      ))
    })
  }
}

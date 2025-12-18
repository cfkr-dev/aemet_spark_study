package DataExtraction.Core.Ifapa

import DataExtraction.Config.{DataExtractionConf, GlobalConf}
import Utils.ChronoUtils
import Utils.ChronoUtils.{await, executeAndAwaitIfTimeNotExceedMinimum}
import Utils.ConsoleLogUtils.Message._
import Utils.HTTPUtils.{buildUrl, sendGetRequest}
import Utils.JSONUtils.lowercaseKeys
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.writeJSON
import sttp.model.Uri
import ujson.{Value, read}

import scala.annotation.tailrec

/**
 * Client for interacting with the IFAPA data sources.
 *
 * This object encapsulates the HTTP interaction and storage logic required to
 * fetch metadata and time-series data for single stations from IFAPA. It
 * leverages project-wide configuration objects and utilities for retries,
 * timing and JSON/storage helpers.
 *
 * Main responsibilities:
 * - Download and persist metadata for single-station meteorological fields.
 * - Download and persist meteorological time-series for a single station.
 * - Download and persist metadata and static information for single stations.
 *
 * Entry point: `ifapaDataExtraction()` executes the full IFAPA extraction
 * workflow (metadata then data) and logs progress to the console.
 */
object IfapaAPIClient {
  private val ctsExecutionIfapa = DataExtractionConf.Constants.execution.ifapaConf
  private val ctsExecutionGlobal = DataExtractionConf.Constants.execution.globalConf
  private val ctsLog = DataExtractionConf.Constants.log.ifapaConf
  private val ctsStorage = DataExtractionConf.Constants.storage.ifapaConf
  private val ctsUrl = DataExtractionConf.Constants.url.ifapaConf
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  /**
   * Perform a GET request to the IFAPA API and return the parsed JSON value.
   *
   * This helper centralizes the HTTP GET call and JSON parsing, returning an
   * Either with an Exception on failure or the parsed ujson.Value on success.
   *
   * @param uri request URI to call
   * @return Right(ujson.Value) with parsed JSON on success, Left(Exception) on failure
   */
  private def getIfapaAPIResource(uri: Uri): Either[Exception, Value] = {
    sendGetRequest(uri) match {
      case Left(exception: Exception) => Left(exception)
      case Right(response) => Right(read(response.body))
    }
  }

  /**
   * Execute the full IFAPA data extraction flow.
   *
   * The flow executes the following steps sequentially and logs progress:
   * 1. Fetch and persist single-station metadata for station info.
   * 2. Fetch and persist single-station metadata for meteorological fields.
   * 3. Fetch and persist static station information (station list/data).
   * 4. Fetch and persist meteorological time-series for a single station.
   *
   * Execution stops if an unhandled exception is thrown by any step. Timing
   * information is recorded and there is a pause between stages defined in
   * global configuration.
   */
  def ifapaDataExtraction(): Unit = {
    chronometer.start()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationInfoStartFetchingMetadata)
    SingleStationInfo.saveSingleStationInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationMeteoInfoStartFetchingMetadata)
    SingleStationMeteoInfo.saveSingleStationMeteoInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationMeteoInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationInfoStartFetchingData)
    SingleStationInfo.saveSingleStationInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationInfoEndFetchingData)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationMeteoInfoStartFetchingData)
    SingleStationMeteoInfo.saveSingleStationMeteoInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.singleStationMeteoInfoEndFetchingData)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }

  /**
   * Helper object that manages metadata and time-series fetching for a
   * single station's meteorological information.
   *
   * Responsibilities:
   * - Fetch and persist metadata that describes available fields and formats.
   * - Fetch and persist time-series data for the configured station and date range.
   */
  private object SingleStationMeteoInfo {
    private val ctsStorageSingleMeteoInfo = ctsStorage.singleStationMeteoInfo
    private val ctsExecutionApiResSingleStationMeteoInfo = ctsExecutionIfapa.apiResources.singleStationMeteoInfo

    /**
     * Fetch and persist metadata required to interpret single-station
     * meteorological responses (field properties, formats, etc.).
     *
     * This method retries until success using configured wait periods for
     * transient failures.
     */
    def saveSingleStationMeteoInfoMetadata(): Unit = {
      def saveSingleStationMeteoInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          def getAndSave(endpoint: String, path: String): Boolean = {
            getIfapaAPIResource(
              buildUrl(endpoint)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => writeJSON(
                path,
                lowercaseKeys(
                  json
                  (ctsExecutionIfapa.reqResp.response.metadata)
                  (ctsExecutionIfapa.reqResp.metadata.singleStationMeteoInfo)
                  (ctsExecutionIfapa.reqResp.metadata.fieldProperties)
                )
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsUrl.allMeteadata,
            ctsStorageSingleMeteoInfo.filepaths.metadata
          )) {
            await(ctsExecutionGlobal.delayTimes.requestMetadata)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestMetadata
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveSingleStationMeteoInfoMetadataAction()
    }

    /**
     * Fetch and persist meteorological time-series for a single station.
     *
     * The inner action builds the appropriate request using state and station
     * codes plus date range and optionally an ET0 flag. Transient failures are
     * retried following configured policies.
     */
    def saveSingleStationMeteoInfo(): Unit = {
      def saveSingleStationMeteoInfoAction(
        stateCode: String,
        stationCode: String,
        startDate: String,
        endDate: String,
        getEt0: Boolean = false
      ): Unit = {
        @tailrec
        def doWhileWithGetAndSave(
          stateCode: String,
          stationCode: String,
          startDate: String,
          endDate: String,
          getEt0: Boolean
        ): Unit = {
          def getAndSave(
            endpoint: String,
            stateCode: String,
            stationCode: String,
            startDate: String,
            endDate: String,
            getEt0: Boolean,
            path: String
          ): Boolean = {
            getIfapaAPIResource(
              buildUrl(
                endpoint,
                List(
                  stateCode,
                  stationCode,
                  startDate,
                  endDate,
                  getEt0.toString
                )
              ),
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => writeJSON(
                path,
                lowercaseKeys(json)
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsUrl.singleStationMeteoInfo,
            stateCode,
            stationCode,
            startDate,
            endDate,
            getEt0,
            ctsStorageSingleMeteoInfo.filepaths.data.format(
              ctsExecutionApiResSingleStationMeteoInfo.startDateAltFormat,
              ctsExecutionApiResSingleStationMeteoInfo.endDateAltFormat
            )
          )) {
            await(ctsExecutionGlobal.delayTimes.requestSimple)
            doWhileWithGetAndSave(stateCode, stationCode, startDate, endDate, getEt0)
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestSimple
        ) {
          doWhileWithGetAndSave(stateCode, stationCode, startDate, endDate, getEt0)
        }
      }

      saveSingleStationMeteoInfoAction(
        ctsExecutionApiResSingleStationMeteoInfo.stateCode,
        ctsExecutionApiResSingleStationMeteoInfo.stationCode,
        ctsExecutionApiResSingleStationMeteoInfo.startDate,
        ctsExecutionApiResSingleStationMeteoInfo.endDate
      )
    }
  }

  /**
   * Helper object that manages fetching metadata and static information for
   * single stations (station descriptors and lists).
   *
   * Responsibilities:
   * - Fetch and persist metadata that describes station info fields.
   * - Fetch and persist the static station information for a configured station.
   */
  private object SingleStationInfo {
    private val ctsStorageSingleStationInfo = ctsStorage.singleStationInfo
    private val ctsExecutionApiResSingleStationInfo = ctsExecutionIfapa.apiResources.singleStationInfo

    /**
     * Fetch and persist metadata that describes single-station static fields.
     *
     * Retries until success using configured wait periods for transient failures.
     */
    def saveSingleStationInfoMetadata(): Unit = {
      def saveSingleStationInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          def getAndSave(endpoint: String, path: String): Boolean = {
            getIfapaAPIResource(
              buildUrl(endpoint)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => writeJSON(
                path,
                lowercaseKeys(
                  json
                  (ctsExecutionIfapa.reqResp.response.metadata)
                  (ctsExecutionIfapa.reqResp.metadata.singleStationInfo)
                  (ctsExecutionIfapa.reqResp.metadata.fieldProperties)
                )
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsUrl.allMeteadata,
            ctsStorageSingleStationInfo.filepaths.metadata
          )) {
            await(ctsExecutionGlobal.delayTimes.requestMetadata)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestMetadata
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveSingleStationInfoMetadataAction()
    }

    /**
     * Fetch and persist static station information for a configured station.
     *
     * Retries are performed for transient failures following global policies.
     */
    def saveSingleStationInfo(): Unit = {
      def saveSingleStationInfoAction(
        stateCode: String,
        stationCode: String
      ): Unit = {
        @tailrec
        def doWhileWithGetAndSave(
          stateCode: String,
          stationCode: String
        ): Unit = {
          def getAndSave(
            endpoint: String,
            stateCode: String,
            stationCode: String,
            path: String
          ): Boolean = {
            getIfapaAPIResource(
              buildUrl(
                endpoint,
                List(
                  stateCode,
                  stationCode
                )
              ),
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => writeJSON(
                path,
                lowercaseKeys(json)
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsUrl.singleStationInfo,
            stateCode,
            stationCode,
            ctsStorageSingleStationInfo.filepaths.data.format(
              stateCode, stationCode
            )
          )) {
            await(ctsExecutionGlobal.delayTimes.requestSimple)
            doWhileWithGetAndSave(stateCode, stationCode)
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestSimple
        ) {
          doWhileWithGetAndSave(stateCode, stationCode)
        }
      }

      saveSingleStationInfoAction(
        ctsExecutionApiResSingleStationInfo.stateCode,
        ctsExecutionApiResSingleStationInfo.stateCode
      )
    }
  }
}
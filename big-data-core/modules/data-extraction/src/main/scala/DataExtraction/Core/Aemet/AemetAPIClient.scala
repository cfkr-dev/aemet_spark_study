package DataExtraction.Core.Aemet

import DataExtraction.Config.{DataExtractionConf, GlobalConf}
import Utils.ChronoUtils.{await, executeAndAwaitIfTimeNotExceedMinimum}
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.HTTPUtils._
import Utils.JSONUtils.lowercaseKeys
import Utils.Storage.Core.Storage
import Utils._
import sttp.client4.UriContext
import sttp.model.Uri
import ujson.{Obj, Value}
import Utils.Storage.JSON.JSONStorageBackend.{readJSON, writeJSON}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec

/**
 * Client for interacting with the AEMET API.
 *
 * This object centralizes HTTP calls to the configured endpoints used to
 * retrieve station metadata and meteorological data. It relies on
 * configuration provided by `DataExtractionConf` and `GlobalConf`, and utility
 * modules for timing, logging, JSON handling and storage.
 *
 * Main responsibilities:
 * - Download and persist station metadata and time-range metadata.
 * - Download and persist meteorological data in bounded time windows.
 * - Retry failed requests according to configured backoff policies.
 *
 * The primary entry point for the extraction workflow is `aemetDataExtraction()`.
 */
object AemetAPIClient {
  private val ctsExecutionAemet = DataExtractionConf.Constants.execution.aemetConf
  private val ctsExecutionGlobal = DataExtractionConf.Constants.execution.globalConf
  private val ctsLog = DataExtractionConf.Constants.log.aemetConf
  private val ctsStorage = DataExtractionConf.Constants.storage.aemetConf
  private val ctsUrl = DataExtractionConf.Constants.url.aemetConf
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  private val aemetApiKey: String = ctsGlobalInit.environmentVars.values.aemetOpenapiApiKey.getOrElse(
    throw new Exception(ctsGlobalUtils.errors.environmentVariableNotFound.format(
      ctsGlobalInit.environmentVars.names.aemetOpenapiApiKey
    ))
  )

  /**
   * Execute the full data extraction flow from AEMET.
   *
   * Steps performed (informational messages are logged to the console):
   * 1. Download and save metadata for all stations.
   * 2. Download and save metadata for meteorological ranges.
   * 3. Download and save static station information.
   * 4. Download and save meteorological data in windows of time.
   *
   * The method measures the total execution time using an internal chronometer
   * and applies pauses between stages according to global configuration.
   */
  def aemetDataExtraction(): Unit = {
    chronometer.start()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allStationInfoStartFetchingMetadata)
    AllStationsData.saveStationInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allStationInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allMeteoInfoStartFetchingMetadata)
    AllStationsMeteorologicalDataBetweenDates.saveStationMeteoInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allMeteoInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allStationInfoStartFetchingData)
    AllStationsData.saveStationInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allStationInfoEndFetchingData)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allMeteoInfoStartFetchingData)
    AllStationsMeteorologicalDataBetweenDates.saveStationMeteoInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.allMeteoInfoEndFetchingData)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }

  /**
   * Perform a GET request to the AEMET API and return the resulting JSON.
   *
   * Some AEMET endpoints follow an indirection pattern: the first response
   * contains metadata including a URL to the actual resource containing the
   * data. When `isMetadata` is true, the method will follow that indirection
   * and fetch the final resource.
   *
   * @param uri initial request URI for AEMET.
   * @param isMetadata when true, interpret the first response as metadata and follow the contained data URL.
   * @return Right(ujson.Value) with the final JSON on success, or Left(Exception) on error.
   */
  private def getAemetAPIResource(
    uri: Uri,
    isMetadata: Boolean = false
  ): Either[Exception, ujson.Value] = {
    sendGetRequest(uri) match {
      case Right(response) =>
        val dataParsedToJSON = ujson.read(response.body)
        dataParsedToJSON(ctsExecutionAemet.reqResp.response.stateNumber).num.toInt match {
          case 200 =>
            val requestURIResource: String = if (isMetadata) {
              ctsExecutionAemet.reqResp.response.metadata
            } else {
              ctsExecutionAemet.reqResp.response.data
            }

            sendGetRequest(uri"${dataParsedToJSON(requestURIResource).str}") match {
              case Right(response) => Right(ujson.read(response.body))
              case Left(exception) => Left(exception)
            }

          case _ => Left(new Exception(ctsGlobalUtils.errors.failOnGettingJson.format(uri.toString())))
        }
      case Left(exception) => Left(exception)
    }
  }

  /**
   * Helper object that manages downloading and storing meteorological data
   * for all stations within date ranges.
   *
   * Responsibilities:
   * - Fetch and persist metadata needed to iterate available date windows.
   * - Fetch and persist actual meteorological data segmented by windows.
   */
  private object AllStationsMeteorologicalDataBetweenDates {
    private val ctsStorageAllMeteoInfo = ctsStorage.allMeteoInfo
    private val ctsExecutionApiResAllMeteoInfo = ctsExecutionAemet.apiResources.allMeteoInfo

    /**
     * Build the request URL, fetch from AEMET and write the resulting JSON to `path`.
     *
     * This method handles retries and logs warnings on failures.
     *
     * @param endpoint configured endpoint for the resource.
     * @param startDate window start (ZonedDateTime).
     * @param endDate window end (ZonedDateTime).
     * @param isMetadata when true, the resource is treated as metadata and no post-processing of keys is applied.
     * @param path file path where the JSON will be written.
     * @return true when the operation succeeds and the JSON is written; false otherwise.
     */
    private def getAndSave(
      endpoint: String,
      startDate: ZonedDateTime,
      endDate: ZonedDateTime,
      isMetadata: Boolean,
      path: String
    ): Boolean = {
      getAemetAPIResource(
        buildUrl(
          endpoint,
          List(
            startDate.format(
              DateTimeFormatter.ofPattern(ctsGlobalUtils.formats.dateHourUtc)
            ),
            endDate.format(
              DateTimeFormatter.ofPattern(ctsGlobalUtils.formats.dateHourUtc)
            )
          ),
          List(
            (ctsExecutionAemet.reqResp.request.apiKey, aemetApiKey)
          )
        ), isMetadata = isMetadata
      ) match {
        case Left(exception: Exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
          false
        case Right(json) => writeJSON(
          path,
          if (isMetadata) json else JSONUtils.lowercaseKeys(json)
        ) match {
          case Left(exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
            false
          case Right(_) => true
        }
      }
    }

    /**
     * Download and persist metadata required to iterate over the API time windows
     * (for example, available date ranges and resource URLs).
     *
     * Retries are performed with configured wait intervals for transient failures.
     */
    def saveStationMeteoInfoMetadata(): Unit = {
      def saveStationMeteoInfoMetadataAction(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
        @tailrec
        def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
          if (!getAndSave(
            ctsUrl.allMeteoInfo,
            startDate,
            endDate,
            isMetadata = true,
            ctsStorageAllMeteoInfo.filepaths.metadata)
          ) {
            ChronoUtils.await(ctsExecutionGlobal.delayTimes.requestMetadata)
            doWhileWithGetAndSave(startDate, endDate)
          }
        }

        if (!startDate.isBefore(DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.endDate))) return

        ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestMetadata
        ) {
          doWhileWithGetAndSave(startDate, endDate)
        }
      }

      saveStationMeteoInfoMetadataAction(
        DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.startDate),
        DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.startDate),
      )
    }

    /**
     * Download and persist meteorological data in recursive time windows. The
     * process maintains a record of the last saved `lastEndDate` to continue
     * progress incrementally.
     *
     * Execution strategy:
     * - Read the last saved end date (if present).
     * - Compute the next request window (start..end).
     * - Fetch and save windows until reaching the configured final date.
     */
    def saveStationMeteoInfo(): Unit = {
      @tailrec
      def saveStationMeteoInfoAction(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
        @tailrec
        def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
          if (!getAndSave(
            ctsUrl.allMeteoInfo,
            startDate,
            endDate,
            isMetadata = false,
            ctsStorageAllMeteoInfo.filepaths.data.format(
              startDate.format(
                DateTimeFormatter.ofPattern(ctsGlobalUtils.formats.dateFormatFile)
              ),
              endDate.format(
                DateTimeFormatter.ofPattern(ctsGlobalUtils.formats.dateFormatFile)
              )
            ))
          ) {
            ChronoUtils.await(ctsExecutionGlobal.delayTimes.requestSimple)
            doWhileWithGetAndSave(startDate, endDate)
          }
        }

        if (!startDate.isBefore(DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.endDate))) return

        ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestSimple
        ) {
          doWhileWithGetAndSave(startDate, endDate)
        }

        writeJSON(
          ctsStorageAllMeteoInfo.dirs.metadata + ctsStorageAllMeteoInfo.filenames.lastSavedDate,
          Obj(
            ctsExecutionAemet.reqResp.lastSavedDates.lastEndDate ->
              endDate.format(
                DateTimeFormatter.ofPattern(ctsGlobalUtils.formats.dateHourZoned)
              )
          )
        ) match {
          case Left(exception) => throw exception
          case Right(_) => ()
        }

        saveStationMeteoInfoAction(
          DateUtils.addTime(endDate, 1),
          DateUtils.capDate(
            DateUtils.addTime(DateUtils.addTime(endDate, 1), 14, 23, 59, 59),
            DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.endDate)
          )
        )
      }

      val (startDate, endDate): (ZonedDateTime, ZonedDateTime) = readJSON(
        ctsStorageAllMeteoInfo.filepaths.lastSavedDate
      ) match {
        case Right(json) =>
          val currentEndDate: ZonedDateTime = DateUtils.getDateZonedDateTime(
            ujson.read(json)
            (ctsExecutionAemet.reqResp.lastSavedDates.lastEndDate).str
          )
          val startDate: ZonedDateTime = DateUtils.addTime(
            currentEndDate, 1
          )
          val endDate: ZonedDateTime = DateUtils.capDate(
            DateUtils.addTime(DateUtils.addTime(currentEndDate, 1), 14, 23, 59, 59),
            DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.endDate)
          )

          (startDate, endDate)

        case Left(exception: Exception) =>
          val startDate: ZonedDateTime = DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.startDate)
          val endDate: ZonedDateTime = DateUtils.addTime(
            DateUtils.getDateZonedDateTime(ctsExecutionApiResAllMeteoInfo.startDate), 14, 23, 59, 59
          )

          ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)

          (startDate, endDate)
      }

      saveStationMeteoInfoAction(
        startDate,
        endDate
      )
    }
  }

  /**
   * Helper object that manages downloading and storing static station
   * information (metadata and station list data).
   *
   * Provides methods to fetch station metadata and the full list of stations
   * and write them to the configured storage backend.
   */
  private object AllStationsData {
    private val ctsStorageAllStationInfo = ctsStorage.allStationInfo

    /**
     * Perform the request to the stations endpoint and write the resulting JSON
     * to the given `path`.
     *
     * @param endpoint configured endpoint for station information.
     * @param path destination path where the JSON will be written.
     * @param isMetadata when true, treat the response as metadata and skip key transformations.
     * @return true when the write succeeds; false otherwise.
     */
    private def getAndSave(
      endpoint: String,
      path: String,
      isMetadata: Boolean = false
    ): Boolean = {
      getAemetAPIResource(
        buildUrl(
          endpoint,
          List(),
          List(
            (ctsExecutionAemet.reqResp.request.apiKey, aemetApiKey)
          )
        ),
        isMetadata = isMetadata
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

    /**
     * Download and persist metadata for all stations. Retries until the resource
     * is successfully obtained, respecting configured wait intervals.
     */
    def saveStationInfoMetadata(): Unit = {
      def saveStationInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          if (!getAndSave(
            ctsUrl.allStationInfo,
            ctsStorageAllStationInfo.filepaths.metadata,
            isMetadata = true
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

      saveStationInfoMetadataAction()
    }

    /**
     * Download and persist static station information (full station list).
     * Retries on transient failures according to global configuration.
     */
    def saveStationInfo(): Unit = {
      def saveStationInfoAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          if (!getAndSave(
            ctsUrl.allStationInfo,
            ctsStorageAllStationInfo.filepaths.data,
          )) {
            await(ctsExecutionGlobal.delayTimes.requestSimple)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsExecutionGlobal.delayTimes.requestSimple
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveStationInfoAction()
    }
  }
}

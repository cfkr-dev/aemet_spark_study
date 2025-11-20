package Core.DataExtraction.Aemet

import Config.{DataExtractionConf, GlobalConf}
import Utils.ChronoUtils.{await, executeAndAwaitIfTimeNotExceedMinimum}
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.FileUtils.saveContentToPath
import Utils.HTTPUtils._
import Utils.JSONUtils.{lowercaseKeys, writeJSON}
import Utils._
import sttp.client4.UriContext
import sttp.model.Uri
import ujson.{Obj, Value}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec

object AemetAPIClient {
  private val ctsExecutionAemet = DataExtractionConf.Constants.execution.aemetConf
  private val ctsExecutionGlobal = DataExtractionConf.Constants.execution.globalConf
  private val ctsLog = DataExtractionConf.Constants.log.aemetConf
  private val ctsStorage = DataExtractionConf.Constants.storage.aemetConf
  private val ctsUrl = DataExtractionConf.Constants.url.aemetConf
  private val ctsUtils = GlobalConf.Constants.utils
  private val ctsGlobalUtils = GlobalConf.Constants.utils
  private val chronometer = ChronoUtils.Chronometer()

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

          case _ => Left(new Exception(ctsUtils.errors.failOnGettingJson.format(uri.toString())))
        }
      case Left(exception) => Left(exception)
    }
  }

  private object AllStationsMeteorologicalDataBetweenDates {
    private val ctsStorageAllMeteoInfo = ctsStorage.allMeteoInfo
    private val ctsExecutionApiResAllMeteoInfo = ctsExecutionAemet.apiResources.allMeteoInfo

    private def getAndSave(
      endpoint: String,
      startDate: ZonedDateTime,
      endDate: ZonedDateTime,
      isMetadata: Boolean,
      path: String,
      filename: String
    ): Boolean = {
      getAemetAPIResource(
        buildUrl(
          endpoint,
          List(
            startDate.format(
              DateTimeFormatter.ofPattern(ctsUtils.formats.dateHourUtc)
            ),
            endDate.format(
              DateTimeFormatter.ofPattern(ctsUtils.formats.dateHourUtc)
            )
          ),
          List(
            (ctsExecutionAemet.reqResp.request.apiKey, sys.env.get(ctsExecutionAemet.reqResp.request.apiKeyEnvName) match {
              case Some(apiKey) => apiKey
              case None => throw new Exception(ctsUtils.errors.environmentVariableNotFound.format(ctsExecutionAemet.reqResp.request.apiKeyEnvName))
            })
          )
        ), isMetadata = isMetadata
      ) match {
        case Left(exception: Exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
          false
        case Right(json) => FileUtils.saveContentToPath(
          path,
          filename,
          if (isMetadata) {
            json
          } else {
            JSONUtils.lowercaseKeys(json)
          },
          appendContent = false,
          JSONUtils.writeJSON
        ) match {
          case Left(exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
            false
          case Right(_) => true
        }
      }
    }

    def saveStationMeteoInfoMetadata(): Unit = {
      def saveStationMeteoInfoMetadataAction(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
        @tailrec
        def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
          if (!getAndSave(
            ctsUrl.allMeteoInfo,
            startDate,
            endDate,
            isMetadata = true,
            ctsStorageAllMeteoInfo.dirs.metadata,
            ctsStorageAllMeteoInfo.filenames.metadata)
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
            ctsStorageAllMeteoInfo.dirs.data,
            ctsStorageAllMeteoInfo.filenames.data.format(
              startDate.format(
                DateTimeFormatter.ofPattern(ctsUtils.formats.dateFormatFile)
              ),
              endDate.format(
                DateTimeFormatter.ofPattern(ctsUtils.formats.dateFormatFile)
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

        FileUtils.saveContentToPath(
          ctsStorageAllMeteoInfo.dirs.metadata,
          ctsStorageAllMeteoInfo.filenames.lastSavedDate,
          Obj(
            ctsExecutionAemet.reqResp.lastSavedDates.lastEndDate ->
              endDate.format(
                DateTimeFormatter.ofPattern(ctsUtils.formats.dateHourZoned)
              )
          ),
          appendContent = false,
          JSONUtils.writeJSON
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

      val (startDate, endDate): (ZonedDateTime, ZonedDateTime) = FileUtils.getContentFromPath(
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

  private object AllStationsData {
    private val ctsStorageAllStationInfo = ctsStorage.allStationInfo

    private def getAndSave(endpoint: String, path: String, filename: String, isMetadata: Boolean = false): Boolean = {
      getAemetAPIResource(
        buildUrl(
          endpoint,
          List(),
          List(
            (ctsExecutionAemet.reqResp.request.apiKey, sys.env.get(ctsExecutionAemet.reqResp.request.apiKeyEnvName) match {
              case Some(apiKey) => apiKey
              case None => throw new Exception(ctsUtils.errors.environmentVariableNotFound.format(ctsExecutionAemet.reqResp.request.apiKeyEnvName))
            })
          )
        ),
        isMetadata = isMetadata
      ) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          false
        case Right(json: Value) => saveContentToPath(
          path,
          filename,
          lowercaseKeys(json),
          appendContent = false,
          writeJSON
        ) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            false
          case Right(_) => true
        }
      }
    }

    def saveStationInfoMetadata(): Unit = {
      def saveStationInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          if (!getAndSave(
            ctsUrl.allStationInfo,
            ctsStorageAllStationInfo.dirs.metadata,
            ctsStorageAllStationInfo.filenames.metadata,
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

    def saveStationInfo(): Unit = {
      def saveStationInfoAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          if (!getAndSave(
            ctsUrl.allStationInfo,
            ctsStorageAllStationInfo.dirs.data,
            ctsStorageAllStationInfo.filenames.data,
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

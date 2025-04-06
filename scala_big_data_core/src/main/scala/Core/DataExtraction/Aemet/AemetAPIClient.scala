package Core.DataExtraction.Aemet

import Config.ConstantsV2
import Utils.ChronoUtils.{await, executeAndAwaitIfTimeNotExceedMinimum}
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleMessage}
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
  private val ctsStorageDataAemetGlobal = ConstantsV2.Storage.DataAemet.Global
  private val ctsRemoteReqAemetParamsGlobal = ConstantsV2.RemoteRequest.AemetAPI.Params.Global
  private val ctsLogsAemetGlobal = ConstantsV2.Logs.Aemet.Global

  private val APIKey = FileUtils.getContentFromPath(ctsStorageDataAemetGlobal.FilePaths.apiKey)

  def aemetDataExtraction(): Unit = {
    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allStationInfoStartFetchingMetadata)
    AllStationsData.saveStationInfoMetadata()
    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allStationInfoEndFetchingMetadata)

    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allMeteoInfoStartFetchingMetadata)
    AllStationsMeteorologicalDataBetweenDates.saveStationMeteoInfoMetadata()
    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allMeteoInfoEndFetchingMetadata)

    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allStationInfoStartFetchingData)
    AllStationsData.saveStationInfo()
    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allStationInfoEndFetchingData)

    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allMeteoInfoStartFetchingData)
    AllStationsMeteorologicalDataBetweenDates.saveStationMeteoInfo()
    ConsoleLogUtils.Message.printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsAemetGlobal.AemetDataExtraction.allMeteoInfoEndFetchingData)
  }

  private def getAemetAPIResource(uri: Uri,
                                  isMetadata: Boolean = false
                                 ): Either[Exception, ujson.Value] = {
    sendRequest(uri) match {
      case Right(response) =>
        val dataParsedToJSON = ujson.read(response.body)
        dataParsedToJSON(ctsRemoteReqAemetParamsGlobal.ResponseJSONKeys.stateNumber).num.toInt match {
          case 200 =>
            val requestURIResource: String = if (isMetadata) {
              ctsRemoteReqAemetParamsGlobal.ResponseJSONKeys.metadata
            } else {
              ctsRemoteReqAemetParamsGlobal.ResponseJSONKeys.data
            }

            sendRequest(uri"${dataParsedToJSON(requestURIResource).str}") match {
              case Right(response) => Right(ujson.read(response.body))
              case Left(exception) => Left(exception)
            }

          case _ => Left(new Exception(ctsLogsAemetGlobal.GetAemetResource.failOnGettingJSON.format(uri.toString())))
        }
      case Left(exception) => Left(exception)
    }
  }

  private object AllStationsMeteorologicalDataBetweenDates {
    private val ctsStorageDataAemetAllMeteoInfo = ConstantsV2.Storage.DataAemet.AllMeteoInfo
    private val ctsRemoteReqAemetParamsAllMeteoInfo = ConstantsV2.RemoteRequest.AemetAPI.Params.AllMeteoInfo
    private val ctsRemoteReqAemetURIAllMeteoInfo = ConstantsV2.RemoteRequest.AemetAPI.URI.AllMeteoInfo

    private def getAndSave(endpoint: String,
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
              DateTimeFormatter.ofPattern(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Format.dateHour)
            ),
            endDate.format(
              DateTimeFormatter.ofPattern(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Format.dateHour)
            )
          ),
          List(
            (ctsRemoteReqAemetParamsGlobal.RequestJSONKeys.apiKey, APIKey match {
              case Right(apiKey) => apiKey
              case Left(exception) => throw exception
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
            ctsRemoteReqAemetURIAllMeteoInfo.dataEndpoint,
            startDate,
            endDate,
            isMetadata = true,
            ctsStorageDataAemetAllMeteoInfo.Dirs.metadata,
            ctsStorageDataAemetAllMeteoInfo.FileNames.metadata)
          ) {
            ChronoUtils.await(ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest)
            doWhileWithGetAndSave(startDate, endDate)
          }
        }

        if (!startDate.isBefore(DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.endDate))) return

        ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest
        ) {
          doWhileWithGetAndSave(startDate, endDate)
        }
      }

      saveStationMeteoInfoMetadataAction(
        DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.startDate),
        DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.startDate),
      )
    }

    def saveStationMeteoInfo(): Unit = {
      @tailrec
      def saveStationMeteoInfoAction(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
        @tailrec
        def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime): Unit = {
          if (!getAndSave(
            ctsRemoteReqAemetURIAllMeteoInfo.dataEndpoint,
            startDate,
            endDate,
            isMetadata = false,
            ctsStorageDataAemetAllMeteoInfo.Dirs.dataRegistry,
            ctsStorageDataAemetAllMeteoInfo.FileNames.dataRegistry.format(
              startDate.format(
                DateTimeFormatter.ofPattern(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Format.dateFormatFile)
              ),
              endDate.format(
                DateTimeFormatter.ofPattern(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Format.dateFormatFile)
              )
            ))
          ) {
            ChronoUtils.await(ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenRequest)
            doWhileWithGetAndSave(startDate, endDate)
          }
        }

        if (!startDate.isBefore(DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.endDate))) return

        ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenRequest
        ) {
          doWhileWithGetAndSave(startDate, endDate)
        }

        FileUtils.saveContentToPath(
          ctsStorageDataAemetAllMeteoInfo.Dirs.metadata,
          ctsStorageDataAemetAllMeteoInfo.FileNames.lastSavedDate,
          Obj(
            ctsRemoteReqAemetParamsAllMeteoInfo.LastSavedDatesJSONKeys.lastEndDate ->
              endDate.format(
                DateTimeFormatter.ofPattern(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Format.dateHourRaw)
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
            DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.endDate)
          )
        )
      }

      val (startDate, endDate): (ZonedDateTime, ZonedDateTime) = FileUtils.getContentFromPath(
        ctsStorageDataAemetAllMeteoInfo.FilePaths.lastSavedDate
      ) match {
        case Right(json) =>
          val currentEndDate: ZonedDateTime = DateUtils.getDateZonedDateTime(
            ujson.read(json)
            (ctsRemoteReqAemetParamsAllMeteoInfo.LastSavedDatesJSONKeys.lastEndDate).str
          )
          val startDate: ZonedDateTime = DateUtils.addTime(
            currentEndDate, 1
          )
          val endDate: ZonedDateTime = DateUtils.capDate(
            DateUtils.addTime(DateUtils.addTime(currentEndDate, 1), 14, 23, 59, 59),
            DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.endDate)
          )

          (startDate, endDate)

        case Left(exception: Exception) =>
          val startDate: ZonedDateTime = DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.startDate)
          val endDate: ZonedDateTime = DateUtils.addTime(
            DateUtils.getDateZonedDateTime(ctsRemoteReqAemetParamsAllMeteoInfo.Execution.Args.startDate), 14, 23, 59, 59
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
    private val ctsStorageDataAemetAllStationInfo = ConstantsV2.Storage.DataAemet.AllStationInfo
    private val ctsRemoteReqAemetURIAllStationInfo = ConstantsV2.RemoteRequest.AemetAPI.URI.AllStationInfo

    private def getAndSave(endpoint: String, path: String, filename: String, isMetadata: Boolean = false): Boolean = {
      getAemetAPIResource(
        buildUrl(
          endpoint,
          List(),
          List(
            (ctsRemoteReqAemetParamsGlobal.RequestJSONKeys.apiKey, APIKey match {
              case Right(apiKey) => apiKey
              case Left(exception) => throw exception
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
            ctsRemoteReqAemetURIAllStationInfo.dataEndpoint,
            ctsStorageDataAemetAllStationInfo.Dirs.metadata,
            ctsStorageDataAemetAllStationInfo.FileNames.metadata,
            isMetadata = true
          )) {
            await(ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest
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
            ctsRemoteReqAemetURIAllStationInfo.dataEndpoint,
            ctsStorageDataAemetAllStationInfo.Dirs.dataRegistry,
            ctsStorageDataAemetAllStationInfo.FileNames.dataRegistry,
          )) {
            await(ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenRequest)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqAemetParamsGlobal.Execution.Time.minimumMillisBetweenRequest
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveStationInfoAction()
    }
  }
}

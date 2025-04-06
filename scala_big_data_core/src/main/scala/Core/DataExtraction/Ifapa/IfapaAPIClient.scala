package Core.DataExtraction.Ifapa

import Config.ConstantsV2._
import Utils.ChronoUtils.{await, executeAndAwaitIfTimeNotExceedMinimum}
import Utils.ConsoleLogUtils.Message._
import Utils.FileUtils.saveContentToPath
import Utils.HTTPUtils.{buildUrl, sendRequest}
import Utils.JSONUtils.{lowercaseKeys, writeJSON}
import sttp.model.Uri
import ujson.{Value, read}

import scala.annotation.tailrec

object IfapaAPIClient {
  private val ctsStorageDataIfapaGlobal = Storage.DataIfapa.Global
  private val ctsRemoteReqIfapaParamsGlobal = RemoteRequest.IfapaAPI.Params.Global
  private val ctsLogsIfapaGlobal = Logs.Ifapa.Global

  private def getIfapaAPIResource(uri: Uri): Either[Exception, Value] = {
    sendRequest(uri) match {
      case Left(exception: Exception) => Left(exception)
      case Right(response) => Right(read(response.body))
    }
  }

  def ifapaDataExtraction(): Unit = {
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationInfoStartFetchingMetadata)
    SingleStationInfo.saveSingleStationInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationMeteoInfoStartFetchingMetadata)
    SingleStationMeteoInfo.saveSingleStationMeteoInfoMetadata()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationMeteoInfoEndFetchingMetadata)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationInfoStartFetchingData)
    SingleStationInfo.saveSingleStationInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationInfoEndFetchingData)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationMeteoInfoStartFetchingData)
    SingleStationMeteoInfo.saveSingleStationMeteoInfo()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaGlobal.IfapaDataExtraction.singleStationMeteoInfoEndFetchingData)
  }

  private object SingleStationMeteoInfo {
    private val ctsStorageDataIfapaSingleStationMeteoInfo = Storage.DataIfapa.SingleStationMeteoInfo
    private val ctsRemoteReqIfapaParamsSingleStationMeteoInfo = RemoteRequest.IfapaAPI.Params.SingleStationMeteoInfo
    private val ctsRemoteReqIfapaURISingleStationMeteoInfo = RemoteRequest.IfapaAPI.URI.SingleStationMeteoInfo
    private val ctsRemoteReqIfapaURIAllMetadata = RemoteRequest.IfapaAPI.URI.AllMetadata

    def saveSingleStationMeteoInfoMetadata(): Unit = {
      def saveSingleStationMeteoInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          def getAndSave(endpoint: String, path: String, filename: String): Boolean = {
            getIfapaAPIResource(
              buildUrl(endpoint)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => saveContentToPath(
                path,
                filename,
                lowercaseKeys(
                  json
                  (ctsRemoteReqIfapaParamsGlobal.ResponseJSONKeys.definitions)
                  (ctsRemoteReqIfapaParamsGlobal.Metadata.SchemaJSONKeys.singleStationMeteoInfo)
                  (ctsRemoteReqIfapaParamsGlobal.Metadata.SchemaJSONKeys.DataField)
                ),
                appendContent = false,
                writeJSON
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsRemoteReqIfapaURIAllMetadata.dataEndpoint,
            ctsStorageDataIfapaSingleStationMeteoInfo.Dirs.metadata,
            ctsStorageDataIfapaSingleStationMeteoInfo.FileNames.metadata
          )) {
            await(ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveSingleStationMeteoInfoMetadataAction()
    }

    def saveSingleStationMeteoInfo(): Unit = {
      def saveSingleStationMeteoInfoAction(stateCode: String,
                                           stationCode: String,
                                           startDate: String,
                                           endDate: String,
                                           getEt0: Boolean = false
                                          ): Unit = {
        @tailrec
        def doWhileWithGetAndSave(stateCode: String,
                                  stationCode: String,
                                  startDate: String,
                                  endDate: String,
                                  getEt0: Boolean): Unit = {
          def getAndSave(endpoint: String,
                         stateCode: String,
                         stationCode: String,
                         startDate: String,
                         endDate: String,
                         getEt0: Boolean,
                         path: String,
                         filename: String
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

          if (!getAndSave(
            ctsRemoteReqIfapaURISingleStationMeteoInfo.dataEndpoint,
            stateCode,
            stationCode,
            startDate,
            endDate,
            getEt0,
            ctsStorageDataIfapaSingleStationMeteoInfo.Dirs.dataRegistry,
            ctsStorageDataIfapaSingleStationMeteoInfo.FileNames.dataRegistry.format(
              startDate, endDate
            )
          )) {
            await(ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenRequest)
            doWhileWithGetAndSave(stateCode, stationCode, startDate, endDate, getEt0)
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenRequest
        ) {
          doWhileWithGetAndSave(stateCode, stationCode, startDate, endDate, getEt0)
        }
      }

      saveSingleStationMeteoInfoAction(
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.stateAlmeriaCode,
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.stationTabernasCode,
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.startDate,
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.endDate
      )
    }
  }

  private object SingleStationInfo {
    private val ctsStorageDataIfapaSingleStationInfo = Storage.DataIfapa.SingleStationInfo
    private val ctsRemoteReqIfapaParamsSingleStationInfo = RemoteRequest.IfapaAPI.Params.SingleStationInfo
    private val ctsRemoteReqIfapaURISingleStationInfo = RemoteRequest.IfapaAPI.URI.SingleStationInfo
    private val ctsRemoteReqIfapaURIAllMetadata = RemoteRequest.IfapaAPI.URI.AllMetadata

    def saveSingleStationInfoMetadata(): Unit = {
      def saveSingleStationInfoMetadataAction(): Unit = {
        @tailrec
        def doWhileWithGetAndSave(): Unit = {
          def getAndSave(endpoint: String, path: String, filename: String): Boolean = {
            getIfapaAPIResource(
              buildUrl(endpoint)
            ) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                false
              case Right(json: Value) => saveContentToPath(
                path,
                filename,
                lowercaseKeys(
                  json
                  (ctsRemoteReqIfapaParamsGlobal.ResponseJSONKeys.definitions)
                  (ctsRemoteReqIfapaParamsGlobal.Metadata.SchemaJSONKeys.singleStationInfo)
                  (ctsRemoteReqIfapaParamsGlobal.Metadata.SchemaJSONKeys.DataField)
                ),
                appendContent = false,
                writeJSON
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  false
                case Right(_) => true
              }
            }
          }

          if (!getAndSave(
            ctsRemoteReqIfapaURIAllMetadata.dataEndpoint,
            ctsStorageDataIfapaSingleStationInfo.Dirs.metadata,
            ctsStorageDataIfapaSingleStationInfo.FileNames.metadata
          )) {
            await(ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest)
            doWhileWithGetAndSave()
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenMetadataRequest
        ) {
          doWhileWithGetAndSave()
        }
      }

      saveSingleStationInfoMetadataAction()
    }

    def saveSingleStationInfo(): Unit = {
      def saveSingleStationInfoAction(stateCode: String,
                                      stationCode: String
                                     ): Unit = {
        @tailrec
        def doWhileWithGetAndSave(stateCode: String,
                                  stationCode: String
                                 ): Unit = {
          def getAndSave(endpoint: String,
                         stateCode: String,
                         stationCode: String,
                         path: String,
                         filename: String
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

          if (!getAndSave(
            ctsRemoteReqIfapaURISingleStationInfo.dataEndpoint,
            stateCode,
            stationCode,
            ctsStorageDataIfapaSingleStationInfo.Dirs.dataRegistry,
            ctsStorageDataIfapaSingleStationInfo.FileNames.dataRegistry.format(
              stateCode, stationCode
            )
          )) {
            await(ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenRequest)
            doWhileWithGetAndSave(stateCode, stationCode)
          }
        }

        executeAndAwaitIfTimeNotExceedMinimum(
          ctsRemoteReqIfapaParamsGlobal.Execution.Time.minimumMillisBetweenRequest
        ) {
          doWhileWithGetAndSave(stateCode, stationCode)
        }
      }

      saveSingleStationInfoAction(
        ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stateAlmeriaCode,
        ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stationTabernasCode
      )
    }
  }
}
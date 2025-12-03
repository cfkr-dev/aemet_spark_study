package Core.DataExtraction.Ifapa

import Config.{DataExtractionConf, GlobalConf}
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

  private def getIfapaAPIResource(uri: Uri): Either[Exception, Value] = {
    sendGetRequest(uri) match {
      case Left(exception: Exception) => Left(exception)
      case Right(response) => Right(read(response.body))
    }
  }

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

  private object SingleStationMeteoInfo {
    private val ctsStorageSingleMeteoInfo = ctsStorage.singleStationMeteoInfo
    private val ctsExecutionApiResSingleStationMeteoInfo = ctsExecutionIfapa.apiResources.singleStationMeteoInfo

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
              startDate, endDate
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

  private object SingleStationInfo {
    private val ctsStorageSingleStationInfo = ctsStorage.singleStationInfo
    private val ctsExecutionApiResSingleStationInfo = ctsExecutionIfapa.apiResources.singleStationInfo

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
package Core.DataExtraction.Ifapa

import Config.ConstantsV2._
import Utils.ConsoleLogUtils.Message._
import Utils.FileUtils.{copyFile, saveContentToPath}
import Utils.JSONUtils.{buildJSONFromSchemaAndData, readJSON, transformJSONValues, writeJSON}
import ujson.{Arr, Obj, Value}

object IfapaToAemetConverter {
  private val ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys = RemoteRequest.AemetAPI.Params.Global.Metadata.SchemaJSONKeys
  private val ctsLogsIfapaAemetFormatGlobal = Logs.IfapaAemetFormat.Global

  private def genEmptyAemetJSONFromMetadata(metadataJSON: ujson.Value): ujson.Value = {
    ujson.Obj.from(
      metadataJSON(ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys.fieldDefJKey).arr.map(field => {
        field(ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys.DataFieldsJSONKeys.idJKey).str -> ujson.Null
      })
    )
  }

  def ifapaToAemetConversion(): Unit = {
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaAemetFormatGlobal.IfapaToAemet.singleStationInfoStartConverting)
    SingleStationInfo.saveSingleStationInfoAemetFormat()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaAemetFormatGlobal.IfapaToAemet.singleStationInfoEndConverting)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaAemetFormatGlobal.IfapaToAemet.singleStationMeteoInfoStartConverting)
    SingleStationMeteoInfo.saveSingleStationMeteoInfoAemetFormat()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsIfapaAemetFormatGlobal.IfapaToAemet.singleStationMeteoInfoEndConverting)
  }

  private object SingleStationMeteoInfo {
    private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.IfapaAPI.Params.SingleStationMeteoInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStationMeteoInfo = RemoteRequest.IfapaAPI.Params.SingleStationMeteoInfo
    private val ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldJSONKeys = RemoteRequest.IfapaAPI.Params.SingleStationInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStateInfoMetadataDataFieldJSONKeys = RemoteRequest.IfapaAPI.Params.SingleStateInfo.Metadata.DataFieldsJSONKeys
    private val ctsStorageDataAemetAllMeteoInfo = Storage.DataAemet.AllMeteoInfo
    private val ctsStorageDataIfapaSingleStationMeteoInfo = Storage.DataIfapa.SingleStationMeteoInfo
    private val ctsStorageDataIfapaSingleStationInfo = Storage.DataIfapa.SingleStationInfo
    private val ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo = Storage.DataIfapaAemetFormat.SingleStationMeteoInfo

    def saveSingleStationMeteoInfoAemetFormat(): Unit = {
      def genAemetMeteoInfoJSONKeysToIfapaJSONValues(ifapaMeteoInfo: ujson.Value, ifapaStationInfo: ujson.Value): Map[String, ujson.Value] = {
        Map(
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.fechaJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.fechaJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.indicativoJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldJSONKeys.codigoestacionJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.nombreJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldJSONKeys.nombreJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.provinciaJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldJSONKeys.provinciaJKey)
            (ctsRemoteReqIfapaParamsSingleStateInfoMetadataDataFieldJSONKeys.nombreJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.altitudJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldJSONKeys.altitudJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.tempmediaJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.precipitacionJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tminJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.tempminJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatminJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.hormintempminJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmaxJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.tempmaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatmaxJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.hormintempmaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.dirJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.dirvientovelmaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.velvientoJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.rachaJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.velvientomaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horarachaJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.horminvelmaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.radiacionJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.humedadmediaJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmaxJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.humedadmaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrmaxJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.horminhummaxJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrminJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.humedadminJKey),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrminJKey ->
            ifapaMeteoInfo(ctsRemoteReqIfapaParamsSingleStationMeteoInfoMetadataDataFieldsJSONKeys.horminhumminJKey)
        )
      }

      def genAemetMeteoInfoFormatters(): Map[String, ujson.Value => ujson.Value] = {
        Map(
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.fechaJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.indicativoJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.nombreJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.provinciaJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.altitudJKey -> (value => (Math.round(value.num * 10) / 10.0).toInt.toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tminJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatminJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmaxJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatmaxJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.dirJKey -> (value => f"${Math.round(value.num / 10.0).toInt}%02d"),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.rachaJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horarachaJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmaxJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrmaxJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrminJKey -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrminJKey -> (value => value.str)
        )
      }

      val aemetAllMeteoInfoMetadata: Value = readJSON(ctsStorageDataAemetAllMeteoInfo.FilePaths.metadata) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val ifapaSingleStationInfo: Value = readJSON(ctsStorageDataIfapaSingleStationInfo.FilePaths.dataRegistry.format(
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.stateAlmeriaCode,
        ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.stationTabernasCode,
      )) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val formatter: Map[String, ujson.Value => ujson.Value] = genAemetMeteoInfoFormatters()

      saveContentToPath(
        ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.Dirs.dataRegistry,
        ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.FileNames.dataRegistry.format(
          ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.startDate,
          ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.endDate
        ),
        readJSON(ctsStorageDataIfapaSingleStationMeteoInfo.FilePaths.dataRegistry.format(
          ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.startDate,
          ctsRemoteReqIfapaParamsSingleStationMeteoInfo.Execution.Args.endDate
        )) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            return
          case Right(json: Value) => Arr(json.arr.map {
            registryJSON => transformJSONValues(
              buildJSONFromSchemaAndData(
                genEmptyAemetJSONFromMetadata(aemetAllMeteoInfoMetadata),
                genAemetMeteoInfoJSONKeysToIfapaJSONValues(
                  registryJSON,
                  ifapaSingleStationInfo
                )
              ),
              formatter
            )
          })
        },
        appendContent = false,
        writeJSON
      ) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => copyFile(
          ctsStorageDataAemetAllMeteoInfo.FilePaths.metadata,
          ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.FilePaths.metadata
        ) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      }
    }
  }

  private object SingleStationInfo {
    private val ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllStationInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys = RemoteRequest.IfapaAPI.Params.SingleStationInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStateInfoMetadataDataFieldsJSONKeys = RemoteRequest.IfapaAPI.Params.SingleStateInfo.Metadata.DataFieldsJSONKeys
    private val ctsRemoteReqIfapaParamsSingleStationInfo = RemoteRequest.IfapaAPI.Params.SingleStationInfo
    private val ctsStorageDataAemetAllStationInfo = Storage.DataAemet.AllStationInfo
    private val ctsStorageDataIfapaSingleStationInfo = Storage.DataIfapa.SingleStationInfo
    private val ctsStorageDataIfapaAemetFormatSingleStationInfo = Storage.DataIfapaAemetFormat.SingleStationInfo

    def saveSingleStationInfoAemetFormat(): Unit = {
      def genAemetAllStationInfoJSONKeysToIfapaJSONValues(ifapaStationInfo: ujson.Value): Map[String, ujson.Value] = {
        Map(
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.latitudJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.latitudJKey),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.provinciaJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.provinciaJKey)
            (ctsRemoteReqIfapaParamsSingleStateInfoMetadataDataFieldsJSONKeys.nombreJKey),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.indicativoJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.codigoestacionJKey),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.altitudJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.altitudJKey),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.nombreJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.nombreJKey),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.indsinopJKey -> "",
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.longitudJKey ->
            ifapaStationInfo(ctsRemoteReqIfapaParamsSingleStationInfoMetadataDataFieldsJSONKeys.longitudJKey)
        )
      }

      def genAemetStationInfoFormatters(): Map[String, ujson.Value => ujson.Value] = {
        Map(
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.latitudJKey -> (value => value.str.replaceAll("(\\d{3})([A-Z])$", "$2")),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.provinciaJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.indicativoJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.altitudJKey -> (value => (Math.round(value.num * 10) / 10.0).toInt.toString.replace(".", ",")),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.nombreJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.indsinopJKey -> (value => value.str),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.longitudJKey -> (value => value.str.replaceAll("(\\d{3})([A-Z])$", "$2"))
        )
      }

      val aemetAllStationInfoMetadata: Value = readJSON(ctsStorageDataAemetAllStationInfo.FilePaths.metadata) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val formatter: Map[String, ujson.Value => ujson.Value] = genAemetStationInfoFormatters()

      saveContentToPath(
        ctsStorageDataIfapaAemetFormatSingleStationInfo.Dirs.dataRegistry,
        ctsStorageDataIfapaAemetFormatSingleStationInfo.FileNames.dataRegistry.format(
          ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stateAlmeriaCode,
          ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stationTabernasCode
        ),
        readJSON(ctsStorageDataIfapaSingleStationInfo.FilePaths.dataRegistry.format(
          ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stateAlmeriaCode,
          ctsRemoteReqIfapaParamsSingleStationInfo.Execution.Args.stationTabernasCode
        )) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            return
          case Right(json: Value) => transformJSONValues(
            buildJSONFromSchemaAndData(
              genEmptyAemetJSONFromMetadata(aemetAllStationInfoMetadata),
              genAemetAllStationInfoJSONKeysToIfapaJSONValues(json)
            ),
            formatter
          )
        },
        appendContent = false,
        writeJSON
      ) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => copyFile(
          ctsStorageDataAemetAllStationInfo.FilePaths.metadata,
          ctsStorageDataIfapaAemetFormatSingleStationInfo.FilePaths.metadata
        ) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      }
    }
  }
}

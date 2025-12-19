package DataExtraction.Core.Ifapa

import DataExtraction.Config.{DataExtractionConf, GlobalConf}
import Utils.ChronoUtils
import Utils.ConsoleLogUtils.Message._
import Utils.JSONUtils.{buildJSONFromSchemaAndData, transformJSONValues}
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.{copyJSON, readJSON, writeJSON}
import ujson.{Arr, Value}

object IfapaToAemetConverter {
  private val ctsExecutionAemet = DataExtractionConf.Constants.execution.aemetConf
  private val ctsLogs = DataExtractionConf.Constants.log.ifapaAemetFormatConf
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  /**
   * Generate an empty AEMET-formatted JSON object based on the AEMET metadata
   * schema. The resulting object contains keys for every field defined in the
   * AEMET schema with null values. This is used as a template to build
   * records mapped from IFAPA data.
   *
   * @param metadataJSON metadata JSON that contains the AEMET schema definition
   * @return an ujson.Value (object) with keys from the AEMET schema and null values
   */
  private def genEmptyAemetJSONFromMetadata(metadataJSON: ujson.Value): ujson.Value = {
    ujson.Obj.from(
      metadataJSON(ctsExecutionAemet.reqResp.metadata.schemaDef).arr.map(field => {
        field(ctsExecutionAemet.reqResp.metadata.fieldId).str -> ujson.Null
      })
    )
  }

  /**
   * Convert IFAPA-stored data into the AEMET-compatible JSON format and
   * persist the converted files to storage.
   *
   * The method orchestrates conversion for both static station info and
   * meteorological time-series, timing the operations and logging progress.
   */
  def ifapaToAemetConversion(): Unit = {
    chronometer.start()

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.singleStationInfoStartConverting)
    SingleStationInfo.saveSingleStationInfoAemetFormat()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.singleStationInfoEndConverting)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.singleStationMeteoInfoStartConverting)
    SingleStationMeteoInfo.saveSingleStationMeteoInfoAemetFormat()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.singleStationMeteoInfoEndConverting)

    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
    Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
  }

  /**
   * Helper object responsible for converting IFAPA meteorological time-series
   * for a single station into the AEMET schema.
   *
   * Responsibilities:
   * - Map IFAPA record fields to the AEMET schema keys.
   * - Apply formatting/transformations to values (e.g., numeric rounding,
   *   string normalization).
   * - Write the converted series to the configured AEMET-format storage path.
   */
  private object SingleStationMeteoInfo {
    private val ctsSchemaAemetAllMeteoInfo = GlobalConf.Constants.schema.aemetConf.allMeteoInfo
    private val ctsSchemaIfapaSingleStationMeteoInfo = GlobalConf.Constants.schema.ifapaConf.singleStationMeteoInfo
    private val ctsSchemaIfapaSingleStationInfo = GlobalConf.Constants.schema.ifapaConf.singleStationInfo
    private val ctsSchemaIfapaSingleStateInfo = GlobalConf.Constants.schema.ifapaConf.singleStateInfo
    private val ctsExecutionIfapa = DataExtractionConf.Constants.execution.ifapaConf
    private val ctsStorageAemetAllMeteoInfo = DataExtractionConf.Constants.storage.aemetConf.allMeteoInfo
    private val ctsStorageIfapaSingleStationMeteoInfo = DataExtractionConf.Constants.storage.ifapaConf.singleStationMeteoInfo
    private val ctsStorageIfapaSingleStationInfo = DataExtractionConf.Constants.storage.ifapaConf.singleStationInfo
    private val ctsStorageIfapaAemetFormatSingleStationMeteoInfo = DataExtractionConf.Constants.storage.ifapaAemetFormatConf.singleStationMeteoInfo

    def saveSingleStationMeteoInfoAemetFormat(): Unit = {
      def genAemetMeteoInfoJSONKeysToIfapaJSONValues(
        ifapaMeteoInfo: ujson.Value,
        ifapaStationInfo: ujson.Value
      ): Map[String, ujson.Value] = {
        Map(
          ctsSchemaAemetAllMeteoInfo.fecha ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.fecha),
          ctsSchemaAemetAllMeteoInfo.indicativo ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.codigoEstacion),
          ctsSchemaAemetAllMeteoInfo.nombre ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.nombre),
          ctsSchemaAemetAllMeteoInfo.provincia ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.provincia)
            (ctsSchemaIfapaSingleStateInfo.nombre),
          ctsSchemaAemetAllMeteoInfo.altitud ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.altitud),
          ctsSchemaAemetAllMeteoInfo.tMed ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.tempMedia),
          ctsSchemaAemetAllMeteoInfo.prec ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.precipitacion),
          ctsSchemaAemetAllMeteoInfo.tMin ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.tempMin),
          ctsSchemaAemetAllMeteoInfo.horaTMin ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.horMinTempMin),
          ctsSchemaAemetAllMeteoInfo.tMax ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.tempMax),
          ctsSchemaAemetAllMeteoInfo.horaTMax ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.horMinTempMax),
          ctsSchemaAemetAllMeteoInfo.dir ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.dirVientoVelMax),
          ctsSchemaAemetAllMeteoInfo.velMedia ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.velViento),
          ctsSchemaAemetAllMeteoInfo.racha ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.velVientoMax),
          ctsSchemaAemetAllMeteoInfo.horaRacha ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.horMinVelMax),
          ctsSchemaAemetAllMeteoInfo.hrMedia ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.humedadMedia),
          ctsSchemaAemetAllMeteoInfo.hrMax ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.humedadMax),
          ctsSchemaAemetAllMeteoInfo.horaHrMax ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.horMinHumMax),
          ctsSchemaAemetAllMeteoInfo.hrMin ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.humedadMin),
          ctsSchemaAemetAllMeteoInfo.horaHrMin ->
            ifapaMeteoInfo(ctsSchemaIfapaSingleStationMeteoInfo.horMinHumMin)
        )
      }

      def genAemetMeteoInfoFormatters(): Map[String, ujson.Value => ujson.Value] = {
        Map(
          ctsSchemaAemetAllMeteoInfo.fecha -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.indicativo -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.nombre -> (value => value.str.toUpperCase),
          ctsSchemaAemetAllMeteoInfo.provincia -> (value => value.str.toUpperCase),
          ctsSchemaAemetAllMeteoInfo.altitud -> (value => Math.round(value.num).toInt.toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.tMed -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.prec -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.tMin -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.horaTMin -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.tMax -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.horaTMax -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.dir -> (value => f"${Math.round(value.num / 10.0).toInt}%02d"),
          ctsSchemaAemetAllMeteoInfo.velMedia -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.racha -> (value => (Math.round(value.num * 10) / 10.0).toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.horaRacha -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.hrMedia -> (value => Math.round(value.num).toInt.toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.hrMax -> (value => Math.round(value.num).toInt.toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.horaHrMax -> (value => value.str),
          ctsSchemaAemetAllMeteoInfo.hrMin -> (value => Math.round(value.num).toInt.toString.replace(".", ",")),
          ctsSchemaAemetAllMeteoInfo.horaHrMin -> (value => value.str)
        )
      }

      val aemetAllMeteoInfoMetadata: Value = readJSON(ctsStorageAemetAllMeteoInfo.filepaths.metadata) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val ifapaSingleStationInfo: Value = readJSON(ctsStorageIfapaSingleStationInfo.filepaths.data.format(
        ctsExecutionIfapa.apiResources.singleStationMeteoInfo.stateCode,
        ctsExecutionIfapa.apiResources.singleStationMeteoInfo.stationCode
      )) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val formatter: Map[String, ujson.Value => ujson.Value] = genAemetMeteoInfoFormatters()

      writeJSON(
        ctsStorageIfapaAemetFormatSingleStationMeteoInfo.filepaths.data.format(
          ctsExecutionIfapa.apiResources.singleStationMeteoInfo.startDateAltFormat,
          ctsExecutionIfapa.apiResources.singleStationMeteoInfo.endDateAltFormat
        ),
        readJSON(ctsStorageIfapaSingleStationMeteoInfo.filepaths.data.format(
          ctsExecutionIfapa.apiResources.singleStationMeteoInfo.startDateAltFormat,
          ctsExecutionIfapa.apiResources.singleStationMeteoInfo.endDateAltFormat
        )) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            return
          case Right(json: Value) => Arr(json.arr.map {
            recordJSON =>
              transformJSONValues(
                buildJSONFromSchemaAndData(
                  genEmptyAemetJSONFromMetadata(aemetAllMeteoInfoMetadata),
                  genAemetMeteoInfoJSONKeysToIfapaJSONValues(
                    recordJSON,
                    ifapaSingleStationInfo
                  )
                ),
                formatter
              )
          })
        }
      ) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => copyJSON(
          ctsStorageAemetAllMeteoInfo.filepaths.metadata,
          ctsStorageIfapaAemetFormatSingleStationMeteoInfo.filepaths.metadata
        ) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      }
    }
  }

  /**
   * Helper object responsible for converting IFAPA static station information
   * into the AEMET-required station-info schema.
   *
   * Responsibilities:
   * - Map IFAPA station fields to AEMET station keys.
   * - Apply formatters to transform values to the expected AEMET presentation.
   * - Persist the converted station data and copy the AEMET metadata template.
   */
  private object SingleStationInfo {
    private val ctsSchemaAemetAllStationInfo = GlobalConf.Constants.schema.aemetConf.allStationInfo
    private val ctsSchemaIfapaSingleStationInfo = GlobalConf.Constants.schema.ifapaConf.singleStationInfo
    private val ctsSchemaIfapaSingleStateInfo = GlobalConf.Constants.schema.ifapaConf.singleStateInfo
    private val ctsExecutionIfapa = DataExtractionConf.Constants.execution.ifapaConf
    private val ctsStorageAemetAllStationInfo = DataExtractionConf.Constants.storage.aemetConf.allStationInfo
    private val ctsStorageIfapaSingleStationInfo = DataExtractionConf.Constants.storage.ifapaConf.singleStationInfo
    private val ctsStorageIfapaAemetFormatSingleStationInfo = DataExtractionConf.Constants.storage.ifapaAemetFormatConf.singleStationInfo

    def saveSingleStationInfoAemetFormat(): Unit = {
      def genAemetAllStationInfoJSONKeysToIfapaJSONValues(ifapaStationInfo: ujson.Value): Map[String, ujson.Value] = {
        Map(
          ctsSchemaAemetAllStationInfo.latitud ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.latitud),
          ctsSchemaAemetAllStationInfo.provincia ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.provincia)
            (ctsSchemaIfapaSingleStateInfo.nombre),
          ctsSchemaAemetAllStationInfo.indicativo ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.codigoEstacion),
          ctsSchemaAemetAllStationInfo.altitud ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.altitud),
          ctsSchemaAemetAllStationInfo.nombre ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.nombre),
          ctsSchemaAemetAllStationInfo.indsinop -> "",
          ctsSchemaAemetAllStationInfo.longitud ->
            ifapaStationInfo(ctsSchemaIfapaSingleStationInfo.longitud)
        )
      }

      def genAemetStationInfoFormatters(): Map[String, ujson.Value => ujson.Value] = {
        Map(
          ctsSchemaAemetAllStationInfo.latitud -> (value => value.str.replaceAll("(\\d{3})([A-Z])$", "$2")),
          ctsSchemaAemetAllStationInfo.provincia -> (value => value.str.toUpperCase),
          ctsSchemaAemetAllStationInfo.indicativo -> (value => value.str),
          ctsSchemaAemetAllStationInfo.altitud -> (value => (Math.round(value.num * 10) / 10.0).toInt.toString.replace(".", ",")),
          ctsSchemaAemetAllStationInfo.nombre -> (value => value.str.toUpperCase()),
          ctsSchemaAemetAllStationInfo.indsinop -> (value => value.str),
          ctsSchemaAemetAllStationInfo.longitud -> (value => value.str.replaceAll("(\\d{3})([A-Z])$", "$2"))
        )
      }

      val aemetAllStationInfoMetadata: Value = readJSON(ctsStorageAemetAllStationInfo.filepaths.metadata) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          return
        case Right(json: Value) => json
      }

      val formatter: Map[String, ujson.Value => ujson.Value] = genAemetStationInfoFormatters()

      writeJSON(
        ctsStorageIfapaAemetFormatSingleStationInfo.filepaths.data.format(
          ctsExecutionIfapa.apiResources.singleStationInfo.stateCode,
          ctsExecutionIfapa.apiResources.singleStationInfo.stationCode
        ),
        readJSON(ctsStorageIfapaSingleStationInfo.filepaths.data.format(
          ctsExecutionIfapa.apiResources.singleStationInfo.stateCode,
          ctsExecutionIfapa.apiResources.singleStationInfo.stationCode
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
        }
      ) match {
        case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
        case Right(_) => copyJSON(
          ctsStorageAemetAllStationInfo.filepaths.metadata,
          ctsStorageIfapaAemetFormatSingleStationInfo.filepaths.metadata
        ) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      }
    }
  }
}

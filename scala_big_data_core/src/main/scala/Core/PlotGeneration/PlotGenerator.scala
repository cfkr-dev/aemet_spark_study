package Core.PlotGeneration

import Config.GlobalConf
import Config.PlotGenerationConf
import Utils.ConsoleLogUtils
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage}
import Utils.HTTPUtils.{buildUrl, sendPostRequest}
import Utils.JSONUtils.{readJSON, removeNullKeys}
import sttp.model.Uri
import ujson.Value
import upickle.default.{ReadWriter, writeJs}

object PlotGenerator {
  private val ctsSchemaAutoPlot = GlobalConf.Constants.schema.autoPlotConf
  private val ctsExecution = PlotGenerationConf.Constants.execution
  private val ctsLog = PlotGenerationConf.Constants.log


  private def generateRequestBody[T: ReadWriter](dto: T): Value = {
    removeNullKeys(writeJs(dto))
  }

  private def generatePlot[T: ReadWriter](uri: Uri, dto: T): Unit = {
    sendPostRequest(uri, generateRequestBody(dto)) match {
      case Left(exception: Exception) => ConsoleLogUtils.Message.printlnConsoleMessage(NotificationType.Warning, exception.toString)
      case Right(response) => ConsoleLogUtils.Message.printlnConsoleMessage(
        NotificationType.Information,
        ctsLog.globalConf.dataSaved.format(ujson.read(response.body)(ctsSchemaAutoPlot.response.destPath))
      )
    }
  }

  private def generateStationsPlots(): Unit = {
    val ctsExecutionStations = ctsExecution.stationsConf
    val encloseHalfLength = 35

    // Count evolution
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.globalConf.generatingPlot.format(
      ctsLog.stationsConf.stationCountEvolFromStart
    ), encloseHalfLength = encloseHalfLength)
    generatePlot(buildUrl(ctsExecutionStations.countEvol.uri), ctsExecutionStations.countEvol.body)

    // Count by state
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.globalConf.generatingPlot.format(
      ctsLog.stationsConf.stationCountByState2024
    ), encloseHalfLength = encloseHalfLength)
    generatePlot(buildUrl(ctsExecutionStations.countByState2024.uri), ctsExecutionStations.countByState2024.body)

    // Count by altitude
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.globalConf.generatingPlot.format(
      ctsLog.stationsConf.stationCountByAltitude2024
    ), encloseHalfLength = encloseHalfLength)
    generatePlot(buildUrl(ctsExecutionStations.countByAltitude2024.uri), ctsExecutionStations.countByAltitude2024.body)

  }

  def generate(): Unit = {
    // STATIONS
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.globalConf.startPlotGeneration.format(
      ctsLog.stationsConf.studyName
    ))
    generateStationsPlots()
    printlnConsoleEnclosedMessage(NotificationType.Information, ctsLog.globalConf.endPlotGeneration.format(
      ctsLog.stationsConf.studyName
    ))
  }
}
